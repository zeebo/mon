# transactions
- transactions must fit in memory
- there are no read guarantees
- there is a single global WAL
- an entry contains a list of values of the form (which, action) where which is which data structure to perform action on
- we buffer uncomitted entries in memory
- when transaction commit happens, we actually execute the actions
- this first flushes the list into the wal
- when opening the WAL, we always begin a new segment (pruning)
- all data structures export a `Commit(gen)` method to tell it that all of the writes up til now are part of some generation
- all data structures export a `Comitted() gen` method to query which generation of writes are safely persisted
- they must maintain the property that if g1 < g2, and comitted returns g2, then g1 is also comitted
- we can delete segments whos commit generation is less than all of the data structure's committed generations
- then we have multi datastructure atomic commits (but one writer at a time (not a problem))
- goodbye index consistenty issues

# papers

- Efficient Data Ingestion and Query Processing for LSM-Based Storage Systems (Extended Version)
https://arxiv.org/pdf/1808.08896.pdf
Seems like it's just about doing batch lookups and some optimizations to the bloom filters.
Should read it better, later, because it appears to be about using them as secondary indexes.

Enabling Efficient Updates in KV Storage via Hashing: Design and Performance Evaluation
- https://arxiv.org/pdf/1811.10000.pdf
Separates keys from values in the LSM tree but does it in such a way that it avoids GC overhead
and other issues. Needs more reading.

SILK: Preventing Latency Spikes in Log-Structured Merge Key-Value Stores
- https://www.usenix.org/system/files/atc19-balmau.pdf
Does some good analysis of existing systems and finds where the issues are affecting latency.
Probably good to read and have an understanding of where the land mines are.

The Log-Structured Merge-Bush & the Wacky Continuum
- https://stratos.seas.harvard.edu/files/stratos/files/wackyandthebush.pdf
Exposes a design/parameter/merging tradeoff that maybe allows for really good point reads and
or for really good ranged reads. Maybe worth exploring, but seems complicated.

TRIAD: Creating Synergies Between Memory, Disk and Log in Log Structured Key-Value Stores
- https://www.usenix.org/system/files/conference/atc17/atc17-balmau.pdf
Hyperoptimzie LSM behavior to avoid disk I/o. For example, treat the WAL as level 0 and write
out a small index for the sorting; use HLL to estimate key overlap to decide when
compactions are appropriate; keep the top K hottest keys in memory and avoid flushing them
when possible.

PebblesDB: Building Key-Value Stores using Fragmented Log-Structured Merge Trees
- https://repositories.lib.utexas.edu/bitstream/handle/2152/68212/RAJU-THESIS-2018.pdf?sequence=1
Uses guards at the different LSM levels so that it avoids doing a bunch of compactions
and reducing write amplification. The LSM-trie has lower write amplification when considering
just point queries, though, but maybe this is a good idea in general.

LSM-trie: An LSM-tree-based Ultra-Large Key-Value Store for Small Data
- https://www.usenix.org/system/files/conference/atc15/atc15-paper-wu.pdf
Organizes the data in a hash trie to reduce write amplification and allows for fast point queries.
