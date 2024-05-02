# Testudo
Simple embedded java database

This project implements a simple embedded database engine for java programs.

For now, the purpose of making this library is to practice below items00:

- Reactivity (non-blocking API)
- B+ Tree
- Bloom filters


## Currently working on

- Storage: storing indexes into files
- Header management
- BTree

## Todo

- Prevent "too many open files" issue: since index chunks can grow, its safer to create a better pool for `SynchronisedFileChannel` used -currently- by `IndexFileManager`
- Searching for keys, adding keys, or key values, are all done linearly. Alternatively, we could add/modify using binary search (works better in case of large key sizes) or hold a metadata in node with sorts
- Allocation may require a flag to set that part of storage as "reserved", so write and overwrite can be different. But would this be enough to prevent writing to a location a requester (a part of code that needed allocation) has allocated itself? Or, maybe this is completely wrong. If we make addIndex() sync, and only one thread can allocate space per table, there won't be an issue? Could still be wrong since some other table may have allocated space in same chunk, and that can ruin things (race condition)
- After allocation, table chunk offsets should be updated: any table after the table we allocated for needs to change (increase) their offset position. This is the only possible way to not break all nodes child pointers.
