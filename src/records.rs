use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use memchr::memchr;
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};

use crate::identity::{Campaigns, People, Products};
use crate::input::{DAY, WEEK, Window, csv_chunks, decimal, fields, lines, timestamp, unquote};

pub const SHARDS: usize = 64;
const EVENT_ID_SHARDS: usize = 256;
pub type Buckets<T> = [Vec<T>; SHARDS];

pub fn group_by_index<T>(
    rows: &mut [T],
    counts: &[usize],
    index_of: impl Fn(&T) -> usize,
) -> Vec<usize> {
    let mut ends = Vec::with_capacity(counts.len());
    let mut offset = 0;
    for &count in counts {
        offset += count;
        ends.push(offset);
    }
    let mut next = Vec::with_capacity(counts.len());
    next.push(0);
    next.extend(ends.iter().copied().take(ends.len().saturating_sub(1)));
    for (group, &end) in ends.iter().enumerate() {
        while next[group] < end {
            let index = next[group];
            let target = index_of(&rows[index]);
            if target != group {
                rows.swap(index, next[target]);
            }
            next[target] += 1;
        }
    }
    ends
}

fn buckets<T>() -> Buckets<T> {
    std::array::from_fn(|_| Vec::new())
}

#[inline]
fn key_hash(id: &[u8]) -> u64 {
    let mut hash = FxHasher::default();
    id.hash(&mut hash);
    hash.finish()
}

#[inline]
fn shard(id: &[u8]) -> usize {
    ((key_hash(id) >> 32) as usize) & (SHARDS - 1)
}

#[derive(Default)]
struct Names {
    ids: FxHashMap<Arc<[u8]>, u32>,
    values: Vec<Arc<[u8]>>,
}

pub struct PersonNames([Mutex<Names>; SHARDS]);

impl PersonNames {
    pub fn new() -> Self {
        Self(std::array::from_fn(|_| Mutex::default()))
    }

    pub fn add_linked(&self, person: &[u8]) -> (usize, u32) {
        let bucket = shard(person);
        let mut names = self.0[bucket].lock().unwrap();
        let id = u32::try_from(names.values.len()).expect("too many person IDs");
        names.values.push(Arc::from(person));
        (bucket, id)
    }

    fn intern(&self, person: &[u8]) -> (usize, u32) {
        let bucket = shard(person);
        let mut names = self.0[bucket].lock().unwrap();
        if let Some(&id) = names.ids.get(person) {
            return (bucket, id);
        }

        let name = Arc::from(person);
        let id = u32::try_from(names.values.len()).expect("too many person IDs");
        names.ids.insert(Arc::clone(&name), id);
        names.values.push(name);
        (bucket, id)
    }

    pub fn freeze(self) -> FrozenNames {
        FrozenNames(self.0.map(|names| names.into_inner().unwrap()))
    }
}

pub struct FrozenNames([Names; SHARDS]);

impl FrozenNames {
    fn lookup(&self, person: &[u8]) -> Option<(usize, u32)> {
        let bucket = shard(person);
        self.0[bucket].ids.get(person).map(|&id| (bucket, id))
    }

    pub fn finish(self) -> [Vec<Arc<[u8]>>; SHARDS] {
        self.0.map(|names| names.values)
    }
}

#[derive(Clone, Copy)]
pub struct Event {
    pub person: u32,
    pub time: i64,
    pub amount: u32,
    pub category: u32,
}

struct ValidatedEvent<'a> {
    person: &'a [u8],
    time: i64,
    amount: u32,
    category: u32,
}

#[derive(Clone, Copy)]
pub struct Click<'a> {
    pub person: u32,
    pub id: &'a [u8],
    pub time: i64,
    pub campaign: u32,
}

#[inline]
fn event<'a>(
    row: [&'a [u8]; 7],
    products: &Products<'_>,
    window: Window,
    ingest: i64,
) -> Option<ValidatedEvent<'a>> {
    let [_, person, raw_time, _, kind, product, raw_price] = row;
    if person.is_empty() || product.is_empty() {
        return None;
    }
    let (amount, category) = match kind {
        b"view" | b"cart" => (0, 0),
        b"purchase" => {
            let amount = decimal(raw_price, 10_000_000)? as u32;
            if amount == 0 {
                return None;
            }
            (amount, products.category(product))
        }
        _ => return None,
    };
    let time = timestamp(raw_time)?;
    if time < window.start || time >= window.end || ingest - time > DAY {
        return None;
    }
    Some(ValidatedEvent {
        person,
        time,
        amount,
        category,
    })
}

const INVALID_EVENT_BUCKET: u8 = u8::MAX;

#[derive(Clone, Copy)]
struct IndexedLine {
    hash: u64,
    id_offset: usize,
    ingest: i64,
    time: i64,
    person: u32,
    amount: u32,
    category: u32,
    bucket: u8,
}

impl IndexedLine {
    fn shard(&self) -> usize {
        ((self.hash >> 32) as usize) & (EVENT_ID_SHARDS - 1)
    }
}

struct ChunkIndex {
    ids: Vec<u8>,
    rows: Vec<IndexedLine>,
    ends: Vec<usize>,
}

impl ChunkIndex {
    fn id(&self, row: &IndexedLine) -> &[u8] {
        let tail = &self.ids[row.id_offset..];
        &tail[..memchr(b',', tail).unwrap()]
    }

    fn shard(&self, index: usize) -> &[IndexedLine] {
        let start = if index == 0 { 0 } else { self.ends[index - 1] };
        &self.rows[start..self.ends[index]]
    }
}

pub struct EventPartitions(Vec<ChunkIndex>);

impl EventPartitions {
    pub fn index_bytes(&self) -> usize {
        self.0
            .iter()
            .map(|chunk| chunk.ids.capacity() + chunk.rows.capacity() * size_of::<IndexedLine>())
            .sum()
    }
}

pub fn partition_events(
    files: &[Mmap],
    products: &Products<'_>,
    people: &People<'_>,
    names: &PersonNames,
    window: Window,
) -> EventPartitions {
    let index = csv_chunks(files)
        .into_par_iter()
        .map_init(FxHashMap::default, |local_names, chunk| {
            local_names.clear();
            let mut rows = Vec::with_capacity(chunk.len() / 76);
            let mut counts = [0; EVENT_ID_SHARDS];
            let mut ids = Vec::with_capacity(chunk.len() / 6);
            for values in lines(chunk).filter_map(fields::<7>) {
                let id = values[0];
                if id.is_empty() {
                    continue;
                }
                let ingest = timestamp(values[3]).expect("ingest_ts is a valid UTC timestamp");
                let mut record = IndexedLine {
                    hash: key_hash(id),
                    id_offset: ids.len(),
                    ingest,
                    time: 0,
                    person: 0,
                    amount: 0,
                    category: 0,
                    bucket: INVALID_EVENT_BUCKET,
                };
                // Keep an invalid winner as a tombstone until all IDs are deduplicated.
                if let Some(valid) = event(values, products, window, ingest) {
                    let (bucket, person) = *local_names.entry(valid.person).or_insert_with(|| {
                        people
                            .linked_route(valid.person)
                            .unwrap_or_else(|| names.intern(valid.person))
                    });
                    record.bucket = bucket as u8;
                    record.person = person;
                    record.time = valid.time;
                    record.amount = valid.amount;
                    record.category = valid.category;
                }
                ids.extend_from_slice(id);
                ids.push(b',');
                counts[record.shard()] += 1;
                rows.push(record);
            }
            let mut offset = 0;
            let mut next = [0; EVENT_ID_SHARDS];
            let ends: Vec<_> = counts
                .into_iter()
                .zip(&mut next)
                .map(|(count, start)| {
                    *start = offset;
                    offset += count;
                    offset
                })
                .collect();
            let mut grouped = Vec::with_capacity(rows.len());
            let slots = grouped.spare_capacity_mut();
            for row in rows {
                let position = &mut next[row.shard()];
                slots[*position].write(row);
                *position += 1;
            }
            debug_assert_eq!(next.as_slice(), ends.as_slice());
            // The counts and rows were updated together, so their shard ranges
            // are disjoint and the loop initialized every position below offset.
            unsafe { grouped.set_len(offset) };
            ChunkIndex {
                ids,
                rows: grouped,
                ends,
            }
        })
        .collect();
    EventPartitions(index)
}

pub fn deduplicate_events(partitions: EventPartitions) -> Vec<Vec<Vec<Event>>> {
    let index = partitions.0;
    let routed = (0..EVENT_ID_SHARDS)
        .into_par_iter()
        .map(|shard| {
            let capacity = index.iter().map(|chunk| chunk.shard(shard).len()).sum();
            let mut winners: HashTable<(&ChunkIndex, &IndexedLine)> =
                HashTable::with_capacity(capacity);
            for chunk in &index {
                for row in chunk.shard(shard) {
                    match winners.entry(
                        row.hash,
                        |(prior_chunk, prior)| {
                            prior.hash == row.hash && prior_chunk.id(prior) == chunk.id(row)
                        },
                        |(_, row)| row.hash,
                    ) {
                        Entry::Occupied(mut entry) => {
                            let winner = entry.get_mut();
                            if row.ingest < winner.1.ingest {
                                *winner = (chunk, row);
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert((chunk, row));
                        }
                    }
                }
            }

            let mut output = buckets();
            for (_, row) in winners {
                if row.bucket != INVALID_EVENT_BUCKET {
                    output[row.bucket as usize].push(Event {
                        person: row.person,
                        time: row.time,
                        amount: row.amount,
                        category: row.category,
                    });
                }
            }
            output
        })
        .collect();
    transpose(routed)
}

pub fn clicks<'a>(
    files: &'a [Mmap],
    campaigns: &Campaigns<'_>,
    people: &'a People<'_>,
    names: &FrozenNames,
    window: Window,
) -> Vec<Vec<Vec<Click<'a>>>> {
    // Copies of a click have identical data. Credit takes the first of
    // equivalent ties, and assists union their intervals, so both are
    // idempotent. Streaming identical clicks avoids a separate ID partition.
    let by_person = csv_chunks(files)
        .into_par_iter()
        .map_init(FxHashMap::default, |local, chunk| {
            local.clear();
            let mut routed = buckets();
            for [id, person, campaign, raw_time] in lines(chunk).filter_map(fields::<4>) {
                if id.is_empty() || person.is_empty() {
                    continue;
                }
                let (Some(&campaign), Some(time)) =
                    (campaigns.ids.get(campaign), timestamp(raw_time))
                else {
                    continue;
                };
                if time < window.start - WEEK || time >= window.end {
                    continue;
                }
                let Some((bucket, person)) = *local.entry(person).or_insert_with(|| {
                    people.linked_route(person).or_else(|| names.lookup(person))
                }) else {
                    continue;
                };
                routed[bucket].push(Click {
                    person,
                    id,
                    time,
                    campaign,
                });
            }
            routed
        })
        .collect();
    transpose(by_person)
}

fn transpose<T>(data: Vec<Buckets<T>>) -> Vec<Vec<Vec<T>>> {
    let mut output = (0..SHARDS).map(|_| Vec::new()).collect::<Vec<_>>();
    for source in data {
        for (target, rows) in output.iter_mut().zip(source) {
            if !rows.is_empty() {
                target.push(rows);
            }
        }
    }
    output
}

struct EventClaims<'a> {
    chunks: Vec<&'a [u8]>,
    slots: Vec<AtomicU64>,
    offset_bits: u32,
    reference_mask: u64,
}

impl<'a> EventClaims<'a> {
    fn new(chunks: Vec<&'a [u8]>) -> Option<Self> {
        let bytes = chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
        let widest = chunks.iter().map(|chunk| chunk.len()).max().unwrap_or(0);
        let offset_bits = usize::BITS - widest.leading_zeros();
        let chunk_bits = usize::BITS - chunks.len().leading_zeros();
        // The reference points into an immutable CSV chunk. The remaining
        // bits hold a tag used only to rule out unequal keys quickly.
        if offset_bits > u32::BITS || offset_bits + chunk_bits > 48 {
            return None;
        }
        let reference_mask = (1_u64 << (offset_bits + chunk_bits)) - 1;
        const MAX_TABLE_BYTES: usize = 512 * 1024 * 1024;
        let slots = (bytes / 44).max(1024).next_power_of_two();
        // Larger tables reduce locality and were slower at final scale.
        // Leave that workload to the partitioned implementation.
        if slots > MAX_TABLE_BYTES / size_of::<AtomicU64>() {
            return None;
        }
        let mut table = Vec::with_capacity(slots);
        table.resize_with(slots, || AtomicU64::new(0));
        Some(Self {
            chunks,
            slots: table,
            offset_bits,
            reference_mask,
        })
    }

    fn reference(&self, chunk: usize, offset: u32) -> u64 {
        ((chunk as u64) << self.offset_bits | u64::from(offset)) + 1
    }

    fn linked_fields(&self, reference: u64) -> (&'a [u8], &'a [u8]) {
        let encoded = reference - 1;
        let chunk = &self.chunks[(encoded >> self.offset_bits) as usize];
        let offset_mask = (1_u64 << self.offset_bits) - 1;
        let row = &chunk[(encoded & offset_mask) as usize..];
        // Every published reference comes from fields::<7>, so these
        // delimiter positions and the complete fields are already valid.
        let mut commas = memchr::memchr_iter(b',', row);
        let first = commas.next().unwrap();
        let start = commas.nth(1).unwrap() + 1;
        let stop = commas.next().unwrap();
        (
            unquote(&row[..first]).unwrap(),
            unquote(&row[start..stop]).unwrap(),
        )
    }

    fn claim(
        &self,
        id: &[u8],
        ingest: &[u8],
        reference: u64,
        displaced: &mut Vec<u64>,
    ) -> Option<bool> {
        let hash = key_hash(id);
        let tag = hash & !self.reference_mask;
        let incoming = tag | reference;
        let mask = self.slots.len() - 1;
        let start = hash as usize & mask;
        for probe in 0..512 {
            let slot = &self.slots[(start + probe) & mask];
            let mut prior = slot.load(Relaxed);
            loop {
                if prior != 0 {
                    if prior & !self.reference_mask != tag {
                        break;
                    }
                    let (old_id, old_ingest) = self.linked_fields(prior & self.reference_mask);
                    if old_id != id {
                        break;
                    }
                    // All ingest values use the same canonical UTC format.
                    // Comparing their bytes is equivalent to comparing time.
                    if old_ingest <= ingest {
                        return Some(false);
                    }
                }
                // Slots never change keys and only move to an earlier
                // copy of that key. All referenced CSV bytes are immutable.
                match slot.compare_exchange_weak(prior, incoming, Relaxed, Relaxed) {
                    Ok(old) => {
                        if old != 0 {
                            displaced.push(old & self.reference_mask);
                        }
                        return Some(true);
                    }
                    Err(observed) => prior = observed,
                }
            }
        }
        None
    }
}

struct ClaimedEvent {
    time: i64,
    offset: u32,
    person: u32,
    amount: u32,
    category: u32,
}

const _: () = assert!(size_of::<ClaimedEvent>() == 24);

impl ClaimedEvent {
    fn event(self) -> Event {
        Event {
            person: self.person,
            time: self.time,
            amount: self.amount,
            category: self.category,
        }
    }
}

struct ClaimedChunk {
    rows: Buckets<ClaimedEvent>,
    displaced: Vec<u64>,
}

pub struct RoutedEvents {
    pub people: Vec<Vec<Vec<Event>>>,
    pub temporary_bytes: usize,
}

pub fn route_events_once(
    files: &[Mmap],
    products: &Products<'_>,
    people: &People<'_>,
    names: &PersonNames,
    window: Window,
) -> Option<RoutedEvents> {
    let mut claims = EventClaims::new(csv_chunks(files))?;
    let parts: Option<Vec<_>> = claims
        .chunks
        .par_iter()
        .enumerate()
        .map_init(FxHashMap::default, |local_names, (index, chunk)| {
            local_names.clear();
            let mut rows = buckets();
            let mut displaced = Vec::new();
            let start = chunk.as_ptr() as usize;
            for line in lines(chunk) {
                let Some(values) = fields::<7>(line) else {
                    continue;
                };
                let id = values[0];
                if id.is_empty() {
                    continue;
                }
                let offset = (line.as_ptr() as usize - start) as u32;
                let raw_ingest = values[3];
                if !claims.claim(
                    id,
                    raw_ingest,
                    claims.reference(index, offset),
                    &mut displaced,
                )? {
                    continue;
                }
                let ingest = timestamp(raw_ingest).expect("ingest_ts is a valid UTC timestamp");
                if let Some(valid) = event(values, products, window, ingest) {
                    let (bucket, person) = *local_names.entry(valid.person).or_insert_with(|| {
                        people
                            .linked_route(valid.person)
                            .unwrap_or_else(|| names.intern(valid.person))
                    });
                    rows[bucket].push(ClaimedEvent {
                        offset,
                        person,
                        time: valid.time,
                        amount: valid.amount,
                        category: valid.category,
                    });
                }
            }
            Some(ClaimedChunk { rows, displaced })
        })
        .collect();
    let mut parts = parts?;
    let mut temporary_bytes = claims.slots.capacity() * size_of::<AtomicU64>();
    temporary_bytes += parts
        .iter()
        .flat_map(|part| part.rows.iter())
        .map(|bucket| bucket.capacity() * size_of::<ClaimedEvent>())
        .sum::<usize>();
    drop(std::mem::take(&mut claims.slots));

    let mut displaced: Vec<_> = parts
        .iter_mut()
        .flat_map(|part| std::mem::take(&mut part.displaced))
        .collect();
    displaced.par_sort_unstable();
    let routed = parts
        .into_par_iter()
        .enumerate()
        .map(|(index, part)| {
            let first = claims.reference(index, 0);
            let last = first + claims.chunks[index].len() as u64;
            let start = displaced.partition_point(|&reference| reference < first);
            let stop = start + displaced[start..].partition_point(|&reference| reference < last);
            if start == stop {
                return part
                    .rows
                    .map(|rows| rows.into_iter().map(ClaimedEvent::event).collect());
            }

            let mut mask = vec![0_u64; claims.chunks[index].len().div_ceil(64)];
            for reference in &displaced[start..stop] {
                let offset = (reference - first) as usize;
                mask[offset / 64] |= 1_u64 << (offset % 64);
            }
            part.rows.map(|rows| {
                rows.into_iter()
                    .filter_map(|event| {
                        let offset = event.offset as usize;
                        (mask[offset / 64] & (1_u64 << (offset % 64)) == 0).then(|| event.event())
                    })
                    .collect()
            })
        })
        .collect();
    Some(RoutedEvents {
        people: transpose(routed),
        temporary_bytes,
    })
}

#[cfg(test)]
mod claim_tests {
    use super::*;

    #[test]
    fn concurrent_claims_keep_full_keys_and_the_earliest_copy() {
        const REFERENCE_MASK: u64 = (1 << 60) - 1;
        const INDEX_MASK: u64 = 1023;
        let first = "evt-unique";
        let wanted = key_hash(first.as_bytes()) & (!REFERENCE_MASK | INDEX_MASK);
        let collision = (0..1_000_000)
            .map(|number| format!("evt-{number}"))
            .find(|id| key_hash(id.as_bytes()) & (!REFERENCE_MASK | INDEX_MASK) == wanted)
            .expect("find a distinct full key with the same initial slot and tag");
        let records: Vec<_> = (0..60)
            .rev()
            .map(|second| {
                let id = if second % 2 == 0 { first } else { &collision };
                format!("\"{id}\",user,0,2025-01-01T00:00:{second:02}Z,view,product,0\n")
            })
            .collect();
        let mut claims = EventClaims::new(records.iter().map(|row| row.as_bytes()).collect())
            .expect("encode the synthetic test chunks");
        claims.reference_mask = REFERENCE_MASK;
        assert_eq!(claims.slots.len(), 1024);

        let displaced: Vec<_> = claims
            .chunks
            .par_iter()
            .enumerate()
            .map(|(index, row)| {
                let values = fields::<7>(row.strip_suffix(b"\n").unwrap()).unwrap();
                let mut displaced = Vec::new();
                let reference = claims.reference(index, 0);
                if !claims
                    .claim(values[0], values[3], reference, &mut displaced)
                    .unwrap()
                {
                    displaced.push(reference);
                }
                displaced
            })
            .collect();

        let mut losers: Vec<_> = displaced.into_iter().flatten().collect();
        losers.sort_unstable();
        let expected: Vec<_> = (0..58).map(|index| claims.reference(index, 0)).collect();
        assert_eq!(losers, expected);
        let winners: Vec<_> = claims
            .slots
            .iter()
            .map(|slot| slot.load(Relaxed) & REFERENCE_MASK)
            .filter(|&reference| reference != 0)
            .map(|reference| claims.linked_fields(reference))
            .collect();
        assert_eq!(winners.len(), 2);
        assert!(winners.contains(&(first.as_bytes(), b"2025-01-01T00:00:00Z")));
        assert!(winners.contains(&(collision.as_bytes(), b"2025-01-01T00:00:01Z")));
    }
}
