use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use memchr::memchr;
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};

use crate::identity::{Campaigns, People, Products};
use crate::input::{DAY, ParsedRow, WEEK, Window, csv_chunks, decimal, lines, timestamp};

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

    fn lookup(&self, person: &[u8]) -> Option<(usize, u32)> {
        let bucket = shard(person);
        let names = self.0[bucket].lock().unwrap();
        names.ids.get(person).map(|&id| (bucket, id))
    }

    pub fn finish(self) -> [Vec<Arc<[u8]>>; SHARDS] {
        self.0.map(|names| names.into_inner().unwrap().values)
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

type Partitions<'a, const WIDTH: usize> = Vec<Vec<Vec<ParsedRow<'a, WIDTH>>>>;

pub fn partition<const WIDTH: usize>(files: &[Mmap]) -> Partitions<'_, WIDTH> {
    let local = csv_chunks(files)
        .into_par_iter()
        .map(|chunk| {
            let mut rows = buckets();
            for row in lines(chunk).filter_map(ParsedRow::<WIDTH>::parse) {
                let id = row.value(0);
                if !id.is_empty() {
                    rows[shard(id)].push(row);
                }
            }
            rows
        })
        .collect();
    transpose(local)
}

#[inline]
fn event<'a>(
    row: [&'a [u8]; 7],
    products: &Products<'_>,
    people: &'a People<'_>,
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
        person: people.resolve(person),
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

pub fn partition_events(
    files: &[Mmap],
    products: &Products<'_>,
    people: &People<'_>,
    names: &PersonNames,
    window: Window,
) -> EventPartitions {
    let index = csv_chunks(files)
        .into_par_iter()
        .map(|chunk| {
            let mut rows = Vec::with_capacity(chunk.len() / 88);
            let mut counts = [0; EVENT_ID_SHARDS];
            let mut ids = Vec::with_capacity(chunk.len() / 6);
            let mut local_names = FxHashMap::default();
            for line in lines(chunk) {
                let Some(row) = ParsedRow::<7>::parse(line) else {
                    continue;
                };
                let values = row.values();
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
                if let Some(valid) = event(values, products, people, window, ingest) {
                    let (bucket, person) = *local_names
                        .entry(valid.person)
                        .or_insert_with(|| names.intern(valid.person));
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
            let ends = group_by_index(&mut rows, &counts, IndexedLine::shard);
            ChunkIndex { ids, rows, ends }
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
    partitions: Partitions<'a, 4>,
    campaigns: &Campaigns<'_>,
    people: &'a People<'_>,
    names: &PersonNames,
    window: Window,
) -> Vec<Vec<Vec<Click<'a>>>> {
    let by_person = partitions
        .into_par_iter()
        .map(|pieces| {
            let capacity: usize = pieces.iter().map(Vec::len).sum();
            let mut seen =
                rustc_hash::FxHashSet::with_capacity_and_hasher(capacity, Default::default());
            let mut routed = buckets();
            let mut local = FxHashMap::default();
            for piece in pieces {
                for line in piece {
                    let [id, person, campaign, raw_time] = line.values();
                    if !seen.insert(id) || person.is_empty() {
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
                    let name = people.resolve(person);
                    let Some((bucket, person)) =
                        *local.entry(name).or_insert_with(|| names.lookup(name))
                    else {
                        continue;
                    };
                    routed[bucket].push(Click {
                        person,
                        id,
                        time,
                        campaign,
                    });
                }
            }
            routed
        })
        .collect::<Vec<_>>();
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
