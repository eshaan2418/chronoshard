# chronoshard design notes

## Chunk encoding (Gorilla, as in Prometheus' XOR chunks)

| Field | Encoding |
|---|---|
| first timestamp | signed varint (ms) |
| first value | 64 raw bits |
| second timestamp | unsigned varint delta |
| later timestamps | delta-of-delta `dod`: `0` → 1 bit; else prefix `10`/`110`/`1110`/`1111` + 14/17/20/64-bit field |
| later values | XOR with previous: `0` if equal; `10` + meaningful bits if they fit the previous leading/trailing-zero window; `11` + 5-bit leading count + 6-bit length + bits otherwise |

The bucket ranges are asymmetric: [−(2ⁿ⁻¹−1), 2ⁿ⁻¹], matching Prometheus. The decoder must treat only values *above* 2ⁿ⁻¹ as negative. A plain two's-complement sign extension turns a delta-of-delta of exactly +8192 into −8192, which is the bug `TestDeltaOfDeltaBuckets` caught.

## Snapshot format

```
uvarint first_wal_segment_not_covered
uvarint series_count
per series: uvarint id | uvarint n_labels | (lenpref name, lenpref value)* | uvarint n_chunks
            per chunk: uvarint n_samples | uvarint n_bytes | Gorilla bytes
u32 crc32c(everything above)
```

Series IDs are preserved across restarts, because WAL sample records refer to them. On load, every chunk is decoded and re-appended, which recreates the open chunk's encoder state so appends can continue. The snapshot is written to `snapshot.tmp`, fsynced, renamed, and the directory fsynced. Only then are the covered WAL segments deleted.

## PromQL semantics that matter for exact agreement

- **Windows are left-open.** An instant selector takes the newest sample in (t − 5m, t]; a range selector `[r]` takes the samples in (t − r, t]. Both are shifted by `offset`. Stale markers (NaN bit pattern `0x7ff0000000000002`) end a series.
- **`rate`/`increase`/`delta`** proceed in this order:
  1. Compute `last − first`, adding the pre-reset value at each counter reset.
  2. If the window start lies further from the first sample than 1.1× the average sample spacing, *first* cap `durationToStart` at half the average spacing.
  3. *Then*, for counters, cap it again at the extrapolated zero point.
  4. Cap `durationToEnd` the same way as in step 2.
  5. Scale the result by (sampled interval + both durations) / sampled interval, and divide by the range for `rate`.

  Doing steps 2 and 3 in the other order changes results only for series that start inside the window. The differential test caught exactly that.
- **`__name__` is dropped** by functions, arithmetic, and comparisons with `bool`. It is kept by comparison filters and set operations.
- **One-to-one matching** keys on all labels except `__name__`, or on the `on(...)` labels. The result keeps only the `on` labels, or drops the `ignoring` labels. A duplicate on either side is an error.
- **`topk`/`bottomk`** use the same min-heap as Prometheus (`container/heap`, NaN sorts lowest, root replaced only on strictly greater), fed in series *creation* order. Prometheus' engine selects with sorting disabled, so ties at the k-th place resolve identically. A label-sorted input order looks more principled but disagrees on ties.

## What would make it faster

1. Seekable chunk iterators, so a selector decodes only the samples it needs from each chunk.
2. Streaming evaluation for long ranges, instead of loading every selector's data up front.
3. Pooled sample buffers and a fused fast path for `sum(rate(...))`, the most common dashboard query.
