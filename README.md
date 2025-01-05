# Testudo

<p>
  <img width="30%" src="https://github.com/sepgh/testudo/blob/main/.docs/assets/Testudo.png" align="left" />
  
**Simple embedded database for java**


This project is _a practice_ for implementing a minimal database system. The practice includes different indexing mechanisms (B+Tree and Bitmaps for example), 
dealing with data storage on disk (Page and Page buffers), caching (LRU), locking (Reader-Writer Lock), parsing and other topics.


<br clear="left"/>
</p>



## Introduction

> Notice: the idea of the project is still evolving! Anything that you may read here is open for changes in future.

The library implemented in this project repository empowers a java application to store data in `collection-field` formats and it provides the sensation - and minimal features - of a database system.

The project is meant to be a practice so that the developer gets involved with some software engineering aspects and gain knowledge, and it may not solve a real world problem that other database implementations (including the embedded ones) don't solve already.

I started studying a tutorial called ["Let's Build a Simple Database"](https://cstack.github.io/db_tutorial/) which is about "writing a sqlite clone from scratch in C" till I met BTree algorithm section.
Later, I understood more about BTree and B+Tree from ["B Trees and B+ Trees. How they are useful in Databases" on Youtube](https://www.youtube.com/watch?v=aZjYr87r1b8), and eventually found myself working on this mini project.

**The thought process, progress and more details of this project is explained on a [youtube playlist called "Write a database from scratch"](https://www.youtube.com/watch?v=HHO2K23XxbM&list=PLWRwj01AnyEtjaw-ZnnAQWnVYPZF5WayV)**. If you are visiting this repository from Youtube, welcome. If not, I suggest you to take a look at the playlist.



## Development Progress

- [X] Basic implementation of Tree-Based [`Unique Index Management`](https://github.com/sepgh/testudo/blob/main/src/main/java/com/github/sepgh/testudo/index/UniqueTreeIndexManager.java) using B+Tree 
- [X] Cluster implementation of B+Tree
- [ ] All "collections" should have a default cluster B+Tree implementation despite their Primary key. Perhaps the Primary Key should point to the cluster id of a collection object (traditionally known as a row of a table).
  - [X] Implement UnsignedLong serializer, which would be cluster key type
  - [ ] Update `SchemeManager` logic
  - [ ] Update `FieldIndexManagerProvider` logic (name should get updated as well, right?)
- [X] Storage management of B+Tree
  - [X] Storage management using `CompactFileIndexStorageManager` and `OrganizedFileIndexStorageManager`
  - [X] Additional Storage Management implementation using `DatabaseStorageManager` (the `DiskDatabaseStorageManager` uses `PageBuffer`s)
- [X] Generic decorator pattern for `UniqueTreeIndexManager`
- [X] LRU Cache implementation for `UniqueTreeIndexManager` decorator. **(More tests required)**
- [X] Providing solution for differentiating `zero` (number) and `null` in a byte array. **(Flag byte is used, which is not the best solution, bitmaps can help here)**
- [X] Non-unique Index Manager
  - [X] B+Tree and `ArrayList` (binary) implementation
  - [X] Bitmap implementation
- [ ] Database Storage Manager
  - [X] Disk Database Storage Manager Basics (Page Buffer)
  - [X] `RemovedObjectTracer` implementations (default: `InMemoryRemovedObjectTracer`) should support splitting the traced objects if the chosen traced position plus requested size is larger than a threshold.
  - [ ] Write Queue to make disk writes (page committing) Async (is it even safe/possible?)
- [X] Query
  - [X] Implement `QueryableInterface` to support quicker query operations on IndexManagers (Done for `LT`, `LTE`, `GT`, `GTE`, `EQ`)
  - [X] Implement Query Class
- [X] Serialization
  - [X] Defining basic serializers for some types including `int`, `long`, `bool`, `char`
  - [X] Model to Scheme Collection conversion
  - [X] Model Serialization
  - [X] Model DeSerialization
- [X] Add support for nullable fields and update model serializer logic
- [ ] Possibly improve `CharArray` serializer to use less space
- [ ] Support primitive data types (ex: models should be able to have `int` field instead of `Integer`)
- [ ] Insertion (and update) verification:
  - [ ] The `insert(byte[])` method of insert operation makes verification more complex, perhaps we should remove it
  - [ ] `primary` index should either have a value or get set as autoincrement
  - [ ] Unique indexes should be verified before any update or insertion is performed (after locking) 
- [X] Database Operations
  - [X] Reader-Writer Lock support at collection level
  - [X] Select Operation
  - [X] Insert Operation
  - [X] Update Operation
  - [X] Delete Operation
- [ ] Either remove IndexIOSession, or improve it to use for `Transaction` support.
- [X] Cache support for cluster index managers
- [ ] Cache support for other indexes. Also find proper usage for `CachedIndexStorageManagerDecorator` or remove it!
- [ ] Exception Throwing and Handling
  - Note: lambdas are going crazy at this point. Use this strategy: https://stackoverflow.com/questions/18198176
- [ ] Shutdown mechanism (gracefully)
- [ ] Logging
- [ ] Performance and Overall Improvements Ideas
  - Update process can be distributed into multiple threads for the `databaseStorage.update()` part
  - There are a bunch of places that we can benefit from Binary Search that have Todos on them.
  - If index ids have a better way of generation, we can use them in index storage managers such as `DiskPageFileIndexStorageManager` to determine the scheme an object belongs to! This also works for storing bitmaps and array lists in db file for `DuplicateIndexManagers`.
  - **Bitmap Space**: use compression or sparse bitmaps


## Open Problems

### Can't perform `query` operation on fields that are not indexed. 

Doing so right now will make us use cluster index, load objects into memory to perform comparisons, and the result would be `Iterator<V>` where V is cluster id.
This means that these objects may later get loaded into memory again. We need a solution to avoid loading objects twice (once for query, once for the higher level operation such as read/update/delete)

**Additional Note**: from performance point of view things are not as awful as it seems:

1. We have a pool of DBObjects per each `page` that is loaded in Page Buffer. Pages may already be in LRU cache, and DBObject pool prevents recreation of new objects in memory (more of a way to reduce memory consumption than performance improvement)
2. We shall use LRU cache for Cluster Index Manager, which means re-reading objects from the cluster index should perform quicker than hitting the disk multiple times.


### Bitmap Max Size

While cluster IDs are set to support `Unsigned Long`, 
current implementation and usage of Bitmap in indexes can't support any value larger than an integer.
This is because a `byte[]` is used and the index passed to an array in java can only be an integer.

### Storing indexes in same file as DB   (DONE | NOT TESTED)

The current implementation has a problem with this, since two instances of `DiskPageDatabaseStorage` will be created and `synchronized` blocks wouldn't perform validly.

Even though current tests work, when we work with multiple collections things will break.
The reason `CollectionSelectInsertOperationMultiThreadedTestCase` can work with Page Buffer is that we lock the collection in `DefaultCollectionInsertOperation`, 
so even though we have multiple instances of `DiskPageDatabaseStorage` (one for DB and one for index), their `.store()` method won't be called from multiple threads.

Note: just using same instance is not enough. The type of the pointer returned by `store` method would be different then. Maybe we should not let the Database Storage Manager handle pointer types? seems totally unnecessary!
