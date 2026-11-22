# Shopping Sessions

Team codec's Rust entry for the Builders Buildathon Databricks Shopping Sessions track.

The pipeline memory-maps read-only CSV files, resolves linked identities with union-find,
and assigns each linked identity a compact person ID. It scans the event files once,
retains invalid copies for the earliest-ingest comparison, and routes only the
surviving winners. It streams valid clicks into person partitions. Each partition
sorts by time, computes sessions, and aggregates last-click credit and seven-day
assists.
The final stage writes the two required Parquet tables with logical DATE columns.

## Build and run

Build the Dockerfile for the scorer's architecture.

```sh
docker build --platform linux/amd64 -t codec-shopping-sessions .
```

Run the example from a local directory containing data, output, and scratch folders.
Start with an empty writable output directory. The entrypoint reads the scorer's DATA_DIR
(default /data), writes to OUTPUT_DIR (default /output), and sizes the Rayon pool
from NCPU. Set SHOPPING_PROFILE=1 to write stage timings to stderr during local profiling.

```sh
docker run --rm --platform linux/amd64 --network none --read-only \
  --cpus 8 --memory 24g --memory-swap 24g --user 1000:1000 \
  --tmpfs /tmp:size=64m \
  -e NCPU=8 -e MEM_GB=24 \
  -v "$PWD/data:/data:ro" -v "$PWD/output:/output" -v "$PWD/scratch:/scratch" \
  codec-shopping-sessions
```

The local data, output, and scratch directories should be outside the submitted
source tree. The output and scratch directories must be writable by UID 1000.

## Dependencies

The pipeline is original team code. It uses Apache Arrow and Parquet for columnar
output, Snappy for compression, Rayon for parallel work, memchr for byte searches,
memmap2 for read-only file mappings, hashbrown and rustc-hash for exact hash tables, and
serde_json for the dataset manifest. Cargo.lock pins the dependency graph.

The official generator, fixtures, and comparator are used for local validation.
