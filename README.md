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

