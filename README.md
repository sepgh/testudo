# Testudo

<p>
  <img width="30%" src="https://github.com/sepgh/testudo/blob/main/.docs/assets/Testudo.png" align="left" />
  
**Simple database embedded to java application**

This project is implemented to practice a B+Tree implementation to index data on database, Bloom Trees to quickly determine existence state of an identifier, and other goodies TBD.

<br clear="left"/>
</p>


## Introduction

>   Notice: the idea of the project is still evolving! Anything that you may read here is open for changes in future.

This project allows a Java application to store data in old fashion table-manner in on an embedded database.
The term, "embedded database" here refers to the fact that the database engine will run directly inside the Java application.

The project is meant to be a practice so that the developer gets involved with some software engineering aspects and gain knowledge, and it may not solve a real world problem that other database implementations (including the embedded ones) don't solve already.

A large portion of the answer to "how databases work" is related to how indexing work, and understanding `B Tree`s or `B+ Tree`s would help a lot as they are implemented at the heart of many existing database systems as well.

I started studying a tutorial called ["Let's Build a Simple Database"](https://cstack.github.io/db_tutorial/) which is about "writing a sqlite clone from scratch in C" till I met BTree algorithm section.
Later, I understood more about BTree and B+Tree from [" B Trees and B+ Trees. How they are useful in Databases" on Youtube](https://www.youtube.com/watch?v=aZjYr87r1b8).

### Indexing

The core component of the system is an implementation of a `B+Tree` to create indexes for the database.
This tree will be stored on disk and in a specific directory per each "database".
The index files can split into different chunks by determining the maximum size of index files. 
This behavior would have an advantage of more I/O throughput (if used wisely) compared to storing all the index data in a single file.
On the other hand, it adds complexity of handling open/close of more files, and preventing the famous "too many open files" issue.

Additionally, each index chunk stores nodes of B+Tree of multiple "tables" (models/entities SMH). So for each table we have a separate B+Tree.

The `IndexStorageManager` interface is in charge of storing data on disk. The engine will be designed to grow a chunk file size only when it's required. This, and the fact that a chunk has data for multiple tables, will result in few more requirements:

1. A header is required to know the offset of the first node byte of each table in each chunk. So far this header is stored in a different file and more details are provided further in this document.
2. Since "growth" happens only when a table has no more space left in a chunk then to allocate `N` bytes at offset `X` we may need to copy data from `X` to the end of the file into a buffer, allocate empty space, and then paste the buffer back into `X+N` position. 
   Despite the fact that this behavior is not performance friendly, the main reason its mentioned here is to bring to the attention that during this allocation phase, no other threads should be allowed to read or write to the file.
3. Additionally, after each growth (due to allocation) at offset `X`, any table that it's first node first byte begins after `X` should update their offset in the header and add `N`.
4. The number of these allocation calls can be reduced since the engine configuration accepts parameters to determine how many empty nodes will be allocated per each allocation call.

To read and write into disk, `AsynchronousFileChannel` is used. While due to characteristics of B+Tree traversal, async reads would not have an advantage (we need to read node A before we know where node B is and read that one), `AsynchronousFileChannel` is still helpful in reusing a single File handler to perform read/write operations in multiple tables.
As mentioned above, a file pool will be required to prevent "too many open file" issues. This pool should have a maximum size, be blocking (async), and allow passing timeout to get file channel or fail otherwise.
Also, to manage resources in a better manner, a specific threadpool may be required to be passed to `AsynchronousFileChannel`s created in the file pool.

For the `IndexManager` we require a [`Reader-Writer lock`](https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock).
Before we discover more about that, it's important to notice that the current interface of `IndexManager` accepts `int table` per each operation, meaning that it's not depended on tables, rather the database itself.
As the name suggests, this lock will allow multiple reads to be performed if no write operation is being performed, as for locks, they will be only performed if there are no read operations performed. Using Reader-Writer lock we can get the benefits of an `IndexManager` in a multithreaded environment.
**Important detail** here would be that this lock can happen on table or database level, but it mainly depends on if we store each table indexes on a different file or a single file. (Todo -__-)

Another thing to note is that with all that's been mentioned, so far we have no atomicity! If one of the read/write operations fail during any of the index management operations that change the disk (add/delete), everything would be lost! That's why concept of `IOSession` is introduced, which is an interface that any IO operation related to indexes should go through and have a `commit()`/`rollback()` mechanism. Additionally, `recover()` functionality may be needed as well.
Of course, the implementation of `IOSession` can become very complex in multithreaded environment. If we store multiple table index in a single file and by any chance we let two threads run an operation that makes changes on the disk at same time, `IOSession` should be shared between these threads so each one can know the changes made by other thread.

Another way to improve B+Tree I/O performance is to add a LRU cache on top of storage layer. Since the storage layer is abstract from node definitions, we could use combination of {table,chunk,offset} as a cache key and byte array of size N as value where node data is stored.
This way, we won't bother reading a node from file if we have "recently" loaded it into memory. This LRU cache may not accept a specific size in bytes but number of the nodes it should store. A single node size may differ due to number of the keys it can hold (degree of the B+Tree), and indexes.


## Development Progress


### Done

- Index management and B+Tree implementation
- Basics of header management sections related to indexes
