# Testudo

<p>
  <img width="300px" src="https://github.com/sepgh/testudo/blob/main/.docs/assets/Testudo.png" align="left" />
  
**Simple database embedded to java application**

This project is implemented to practice a B+Tree implementation to index data on database, Bloom Trees to quickly determine existence state of an identifier, and other goodies TBD.

<br clear="left"/>
</p>



## Currently working on

- Storage: storing indexes into files
- Header management
- B+Tree

## Todo

- Prevent "too many open files" issue: since index chunks can grow, its safer to create a better pool for `SynchronisedFileChannel` used -currently- by `IndexFileManager`
- Searching for keys, adding keys, or key values, are all done linearly. Alternatively, we could add/modify using binary search (works better in case of large key sizes) or hold a metadata in node with sorts
- Allocation may require a flag to set that part of storage as "reserved", so write and overwrite can be different. But would this be enough to prevent writing to a location a requester (a part of code that needed allocation) has allocated itself? Or, maybe this is completely wrong. If we make addIndex() sync, and only one thread can allocate space per table, there won't be an issue? Could still be wrong since some other table may have allocated space in same chunk, and that can ruin things (race condition) | **update**: this may be wrong as BTree operations on a single db should be sync
