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
- [ ] Storage management of B+Tree
  - [X] Storage management using `CompactFileIndexStorageManager` and `OrganizedFileIndexStorageManager`
  - [ ] Additional Storage Management implementation using `DatabaseStorageManager` (the `DiskDatabaseStorageManager` uses `PageBuffer`s)
- [X] Generic decorator pattern for `UniqueTreeIndexManager`
- [X] LRU Cache implementation for `UniqueTreeIndexManager` decorator. **(More tests required)**
- [ ] Tree Index Manager Locks:
  - [ ] Async decorator for `UniqueTreeIndexManager` to decorate multiple objects using a single decorator (locking operations of multiple IndexManagers using a single decorator)
  - [ ] Async decorator for `DuplicateIndexManager` to decorate multiple objects using a single decorator (locking operations of multiple IndexManagers using a single decorator)
  - [ ] The implementation should be able to combine lock for both Duplicate and Unique Index Managers
- [X] Providing solution for differentiating `zero` (number) and `null` in a byte array. **(Flag byte is used, which is not the best solution, bitmaps can help here)**
- [X] Non-unique Index Manager
  - [X] B+Tree and `LinkedList` (binary) implementation
  - [X] Bitmap implementation
- [ ] Database Storage Manager
  - [X] Disk Database Storage Manager Basics (Page Buffer)
  - [ ] Write Queue to make writes Async (is it even safe/possible?) 


