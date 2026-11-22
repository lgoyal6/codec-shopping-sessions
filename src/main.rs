mod analytics;
mod identity;
mod input;
mod output;
mod records;

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use memmap2::Mmap;
use rayon::prelude::*;

use crate::analytics::CampaignTotals;
use crate::identity::{Campaigns, People, Products};
use crate::input::{Window, csv_paths, map_csv};

const HEAP_TRIM_INDEX_BYTES: usize = 2 * 1024 * 1024 * 1024;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn directory(name: &str, default: &str) -> PathBuf {
    env::var_os(name).map_or_else(|| PathBuf::from(default), PathBuf::from)
}

fn load(dir: &Path) -> io::Result<Vec<Mmap>> {
    let mut files = Vec::new();
    for path in csv_paths(dir)? {
        if let Some(file) = map_csv(&path)? {
            files.push(file);
        }
    }
    Ok(files)
}

fn release_unused_heap() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        unsafe extern "C" {
            fn malloc_trim(pad: usize) -> std::ffi::c_int;
        }
        // Event deduplication just dropped the large input index. Return free
        // allocator pages before click sorting starts faulting mapped data in.
        unsafe { malloc_trim(0) };
    }
}

struct Profile {
    enabled: bool,
    start: Instant,
    previous: Instant,
}

impl Profile {
    fn new() -> Self {
        let start = Instant::now();
        Self {
            enabled: env::var_os("SHOPPING_PROFILE").is_some(),
            start,
            previous: start,
        }
    }

    fn mark(&mut self, name: &str) {
        let now = Instant::now();
        if self.enabled {
            eprintln!(
                "stage {name} {:.6} {:.6}",
                (now - self.previous).as_secs_f64(),
                (now - self.start).as_secs_f64()
            );
        }
        self.previous = now;
    }
}

fn main() -> Result<()> {
    let mut profile = Profile::new();
    let data = directory("DATA_DIR", "/data");
    let out = directory("OUTPUT_DIR", "/output");
    let threads = env::var("NCPU").map_or_else(
        |_| Ok(std::thread::available_parallelism().map_or(1, |n| n.get())),
        |value| value.parse::<usize>(),
    )?;
    if threads == 0 {
        return Err(io::Error::other("NCPU must be positive").into());
    }
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()?;

    let window = Window::load(&data.join("manifest.json"))?;
    let link_files = load(&data.join("links"))?;
    let product_file = fs::read(data.join("products.csv"))?;
    let campaign_file = fs::read(data.join("campaigns.csv"))?;
    let products = Products::load(&product_file);
    let campaigns = Campaigns::load(&campaign_file);
    let names = records::PersonNames::new();
    let people = People::from_links(link_files.iter().map(|file| file.as_ref()), |canonical| {
        names.add_linked(canonical)
    });
    profile.mark("dimensions_links");

    let event_files = load(&data.join("events"))?;
    let (events, index_bytes) =
        match records::route_events_once(&event_files, &products, &people, &names, window) {
            Some(routed) => {
                drop(event_files);
                profile.mark("event_stream_exact_dedup_route");
                (routed.people, routed.temporary_bytes)
            }
            None => {
                profile.mark("event_stream_fallback");
                let partitions =
                    records::partition_events(&event_files, &products, &people, &names, window);
                drop(event_files);
                profile.mark("event_scan_validate_partition");
                let bytes = partitions.index_bytes();
                let events = records::deduplicate_events(partitions);
                profile.mark("event_dedup_route");
                (events, bytes)
            }
        };
    if profile.enabled {
        eprintln!("event_index_bytes {index_bytes}");
    }
    if index_bytes >= HEAP_TRIM_INDEX_BYTES {
        release_unused_heap();
        profile.mark("release_event_memory");
    }

    let names = names.freeze();
    let click_files = load(&data.join("clicks"))?;
    let clicks = records::clicks(&click_files, &campaigns, &people, &names, window);
    profile.mark("click_scan_route");

    let names = names.finish();
    let shards = events
        .into_par_iter()
        .zip(clicks)
        .zip(names.par_iter())
        .map(|((events, clicks), names)| {
            analytics::summarize(
                events,
                clicks,
                names,
                products.categories.len(),
                campaigns.names.len(),
                campaigns.organic,
            )
        })
        .collect::<Vec<_>>();
    profile.mark("sort_session_attribution");

    let user_out = out.join("user_daily");
    let campaign_out = out.join("campaign_daily");
    fs::create_dir_all(&user_out)?;
    fs::create_dir_all(&campaign_out)?;
    let (user_days, shard_campaigns): (Vec<_>, Vec<_>) = shards.into_iter().unzip();
    let mut campaign_totals = CampaignTotals::default();
    for part in shard_campaigns {
        for (key, value) in part {
            campaign_totals.entry(key).or_default().merge(value);
        }
    }

    let (users, campaigns) = rayon::join(
        || {
            user_days
                .par_chunks(4)
                .enumerate()
                .try_for_each(|(index, shards)| {
                    if index == 0 || shards.iter().any(|shard| !shard.is_empty()) {
                        output::users(
                            &user_out.join(format!("part-{index:02}.parquet")),
                            shards.iter().flatten(),
                            &products,
                        )?;
                    }
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                })
        },
        || {
            output::campaigns(
                &campaign_out.join("part-00.parquet"),
                campaign_totals,
                &campaigns,
            )
        },
    );
    users?;
    campaigns?;
    profile.mark("parquet");
    Ok(())
}
