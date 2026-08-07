# Research: filesystem_cache as an alternative to moving parts

**Problem.** Hot data drifts onto a colder disk (e.g. object storage). The mover
solves this by physically relocating parts (`ALTER TABLE ... MOVE PART ... TO
DISK`). ClickHouse offers a second way to get hot data onto a fast disk: the
**filesystem cache**.

## What it is

A `cache` disk is a **local read-through cache layered on top of an object
storage disk**. The authoritative data stays on S3; the first read of a file
segment pulls it over the network and stores it on a local fast disk, and every
later read of that segment is served locally. Hot data ends up on a fast disk
**automatically after it is first queried** — no `MOVE`, no manual tiering.

```
query ──▶ cache disk ──hit──▶ local SSD (fast)
                     └─miss──▶ S3 (slow) ──▶ fill cache
```

## Configuration (see `deploy/clickhouse/config.d/storage.xml`)

```xml
<object_storage_cached>
    <type>cache</type>
    <disk>object_storage</disk>            <!-- the S3 disk being cached -->
    <path>/var/lib/clickhouse/disks/s3_cache/</path>
    <max_size>2Gi</max_size>
    <cache_on_write_operations>true</cache_on_write_operations>
</object_storage_cached>
```

A storage policy (`cached`) exposes it; a table created with
`SETTINGS storage_policy = 'cached'` keeps its data on S3 but reads through the
local cache.

## Demo — `cmd/fscache` (`make fscache-demo`)

The stand creates a ~100 MiB table on the `cached` policy, drops the filesystem
cache, then runs the **same scan twice** and reads the per-query stats from
`system.query_log` ProfileEvents:

```
Result — same scan, cache dropped in between:
                            COLD (1st read)    WARM (2nd read)
  server duration (ms)                  101                 21
  read from S3 (source)           95.78 MiB             0.00 B
  read from cache                 54.54 MiB          95.78 MiB
  S3 GET requests                        15                  0

Filesystem cache now holds 15 segment(s), 95.78 MiB.
```

The cold scan pulls ~96 MiB from S3 (15 GETs) and fills the cache; the warm scan
serves the identical data from the local cache with **zero** S3 traffic and runs
~5× faster.

### How it is measured

- The scan is `SELECT count(), sum(cityHash64(payload)) ...`. `cityHash64` over
  the payload forces reading the whole column data. Note: `sum(length(payload))`
  is **not** enough — `length()` reads only the String size substream, so it
  barely touches S3 and hides the effect.
- Each scan is tagged with a unique `SETTINGS log_comment`, so its row is located
  in `system.query_log` after `SYSTEM FLUSH LOGS`.
- The relevant ProfileEvents:
  - `CachedReadBufferReadFromSourceBytes` — bytes fetched from S3 (cache miss);
  - `CachedReadBufferReadFromCacheBytes` — bytes served from the local cache;
  - `S3GetObject` — number of S3 GET requests.
- `SYSTEM DROP FILESYSTEM CACHE` resets the cache so the first scan is genuinely
  cold (the insert itself warms it when `cache_on_write_operations` is on).
- `system.filesystem_cache` shows current occupancy (segments and size).

## filesystem_cache vs. moving parts

| | filesystem_cache | `MOVE PART TO DISK` (the mover) |
| --- | --- | --- |
| Data location | stays on S3 (authoritative) | physically on the local disk |
| Hot data on fast disk | after first read, automatically | immediately after the move |
| Local space used | bounded by `max_size` (evicts LRU) | full size of moved parts |
| Survives cache eviction / restart | may need re-warming | yes, it is the data |
| Control | implicit (access pattern driven) | explicit (operator picks parts) |
| Cost | no data rewrite; extra local SSD | rewrites/moves data between disks |

**When cache fits better:** working set ≪ total data, access is read-heavy and
skewed to recent/hot ranges, you want automatic tiering without operations.

**When moving fits better:** you need guaranteed local residency (predictable
latency, no cold-start after eviction), the hot set is well-defined, or you are
reclaiming object storage.

They are complementary: cache handles transient hotness cheaply; the mover pins a
known hot partition to a fast disk deterministically.

## Adding cache to an existing table — without copying data

A common ask: an existing (possibly petabyte-scale) table already lives on a raw
object-storage disk; how do you put a cache in front of it **without rewriting
the data**? The following was verified empirically on ClickHouse 26.3.

### What does NOT work

- **Switch to a cache-only policy** — `ALTER TABLE ... MODIFY SETTING
  storage_policy = 'cached'` fails: `New storage policy 'cached' shall contain
  volumes of the old storage policy` (a new policy must be a superset).
- **Add the cache disk as an extra volume in a superset policy** — fails:
  `New storage policy contain disks which already contain data of a table with
  the same name`. The `cache` disk wraps the *same* S3 path that already holds
  the table's parts, so ClickHouse rejects having both in one policy.
- **`ALTER TABLE ... MOVE ... TO DISK 'the_cache_disk'`** — the disk is not in
  the current policy (`UNKNOWN_DISK`), and it cannot be added (previous point).
- **Inline cache keys on the s3 disk** (`data_cache_enabled`, `data_cache_path`,
  …) — silently ignored; `system.filesystem_cache_settings` shows no such cache.
- **New table on the cached policy + `INSERT SELECT` + `EXCHANGE TABLES`** — works
  and is fine for small tables, but it is a full data copy, so it is unusable at
  petabyte scale.

### What works: an in-place disk swap (config + tiny metadata move)

Key facts that make this cheap and safe:

- A `cache` disk is **transparent** over its underlying disk — it does not change
  the storage layout, only intercepts reads to cache segments locally.
- An s3 disk keeps its **local metadata** (small pointer files → S3 object keys)
  under `/var/lib/clickhouse/disks/<disk_name>/`. This is tiny even when the S3
  data is petabytes.
- A part records the **disk name** it lives on. If we put a cache disk under that
  same name, the existing parts are transparently read through the cache.

Recipe (disk named `s3`, done **per node**, node stopped during the move):

1. In the storage config, rename the existing raw disk to `s3_backing` (keep the
   endpoint/credentials byte-for-byte) and define a **new `s3` of type `cache`**
   wrapping it. The storage policy is untouched — it still references `s3`.

   ```xml
   <s3_backing>
       <type>s3</type>
       <endpoint>...unchanged...</endpoint>
       <access_key_id>...</access_key_id>
       <secret_access_key>...</secret_access_key>
   </s3_backing>

   <s3>
       <type>cache</type>
       <disk>s3_backing</disk>
       <path>/var/lib/clickhouse/disks/s3_cache/</path>
       <max_size>500Gi</max_size>
       <cache_on_write_operations>true</cache_on_write_operations>
   </s3>
   ```

2. Move the (tiny) metadata directory to match the renamed backing disk:

   ```sh
   mv /var/lib/clickhouse/disks/s3 /var/lib/clickhouse/disks/s3_backing
   ```

3. Start the node. Parts still record `disk_name = 's3'`, which now resolves to
   the cache disk → reads go through the local cache; the S3 objects are never
   touched; row counts and data are unchanged.

Verified result (300k-row table): before the swap reads showed zero cache
activity; after the swap the cold read pulled 28.73 MiB from S3 and the warm read
served the same 28.73 MiB entirely from cache (0 B from S3), with identical row
count and checksum — no part moved.

Reproduce the whole swap end-to-end (needs `make up`):

```sh
make fscache-swap-demo   # deploy/scripts/fscache-inplace-swap.sh
```

### Caveats

- **Per replica.** Each node has its own local metadata and config; roll the
  change one replica at a time so the table stays available.
- **Move only while the node is stopped** (`stop → mv → start`), never under a
  running server.
- **Backing endpoint/credentials must be identical** to the old disk, or S3 keys
  won't resolve.
- **Check the metadata path first**: `SELECT path FROM system.parts WHERE
  table = '...' LIMIT 1`. If the disk uses an explicit `<metadata_path>` or a
  separate metadata disk, skip the `mv` and instead point the backing disk's
  metadata at the existing location.
- The cache `<path>` must not collide with the moved metadata directory.
- Re-tune the cache on the new cache disk afterwards (`max_size`, `cache_policy`,
  `bypass_cache_threshold`, …).

### Why moving partitions between tables doesn't help (DETACH/ATTACH)

A tempting idea: create a new table whose S3 volume is the `cache` disk, then move
the partitions over "as is" (they stay on their disks) and swap table names — no
full copy. It does **not** work, because a part is read through the disk it lives
on, so to be cached it must physically end up **on the cache disk** — and the
cache disk is a *different* disk from the raw S3 disk (even pointing at the same
bucket path). Measured on 26.3, source table on `tiered` (partition on `local`,
partition on `object_storage`), destination on a `local` + `object_storage_cached`
policy:

| Operation | Result | S3 ops |
| --- | --- | --- |
| `MOVE PARTITION … TO TABLE` (local **and** S3) | rejected — `should have the same storage policy … tiered vs tiered_cache` (the whole policy is compared, not per part) | — |
| `ATTACH PARTITION FROM` (local → local) | metadata-only (**hardlink**) | `0 copy, 0 PUT, 0 GET` |
| `ATTACH PARTITION FROM` (S3 → **cache disk**) | **physical copy**, data duplicated in the bucket | `0 CopyObject, 19 PUT, 28 GET` |

Why the asymmetry:

- A part is a set of files: for an s3 disk, small local pointer files plus the
  actual objects in the bucket.
- `ATTACH PARTITION FROM` **within the same disk** just hardlinks those files —
  instant, no extra space. That is why the `local → local` move was free.
- `ATTACH PARTITION FROM` **across disks** cannot hardlink; the destination disk
  must own its **own** objects, so ClickHouse re-reads the source (GET) and writes
  new objects (PUT). The data now exists twice in the bucket until the source is
  dropped — and here it was a full read+write, not even a server-side `CopyObject`.
- Sharing the same bucket path does **not** help: ClickHouse treats each disk as
  an independent namespace with per-disk object ownership. The only zero-copy
  object sharing (`allow_remote_fs_zero_copy_replication`) is for **replicas of
  one table on the same disk** across nodes, not for moving a part between disks
  or tables.

So this approach moves the hot (`local`) parts for free — the ones that don't need
caching — while **physically copying exactly the S3 parts you wanted to cache**.
Net: no cheaper than `INSERT SELECT`. The in-place disk swap above is the only
no-copy path, and for a two-disk table you apply it **only to the S3 disk**,
leaving `local` untouched.

## Reproduce

```sh
make up
make fscache-demo        # rebuilds the image and runs cmd/fscache
make down
```
