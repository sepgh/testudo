package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.ErrorMessage;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.InvalidDBObjectWrapper;
import com.github.sepgh.testudo.functional.DBObjectUpdateConsumer;
import com.github.sepgh.testudo.storage.pool.FileHandlerPool;
import com.github.sepgh.testudo.utils.FileUtils;
import lombok.Getter;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.sepgh.testudo.exception.ErrorMessage.*;


@Getter
public class DiskPageDatabaseStorageManager implements DatabaseStorageManager {
    private final PageBuffer pageBuffer;
    private final EngineConfig engineConfig;
    private final FileHandlerPool fileHandlerPool;
    private final RemovedObjectsTracer removedObjectsTracer;
    private volatile PageBuffer.PageTitle lastPageTitle;

    public DiskPageDatabaseStorageManager(EngineConfig engineConfig, FileHandlerPool fileHandlerPool, RemovedObjectsTracer removedObjectsTracer) {
        this.engineConfig = engineConfig;
        this.fileHandlerPool = fileHandlerPool;
        this.removedObjectsTracer = removedObjectsTracer;
        this.pageBuffer = new PageBuffer(
                this.engineConfig.getDbPageBufferSize(),
                this::pageFactory
        );
    }

    public DiskPageDatabaseStorageManager(EngineConfig engineConfig, FileHandlerPool fileHandlerPool) {
        this(engineConfig, fileHandlerPool, new RemovedObjectsTracer.InMemoryRemovedObjectsTracer(engineConfig.getIMROTMinLengthToSplit()));
    }


    public Pointer store(int schemeId, int collectionId, int version, byte[] data) throws InternalOperationException {
        Optional<RemovedObjectsTracer.RemovedObjectLocation> optionalRemovedObjectLocation = this.removedObjectsTracer.getRemovedObjectLocation(DBObject.getWrappedSize(data.length));

        if (optionalRemovedObjectLocation.isPresent()) {
            RemovedObjectsTracer.RemovedObjectLocation removedObjectLocation = optionalRemovedObjectLocation.get();

            // Get page from chunk and file offset. It will enter the buffer
            Page page = this.getBufferedPage(
                    removedObjectLocation.pointer().getChunk(),
                    removedObjectLocation.pointer().getPosition()
            );

            // Find offset of the object within the page
            int offset = (int) (removedObjectLocation.pointer().getPosition() % this.engineConfig.getDbPageSize());

            Optional<DBObject> optionalDBObjectWrapper = page.getDBObjectFromPool(offset, data.length);
            if (optionalDBObjectWrapper.isPresent()) {
                try {
                    this.store(optionalDBObjectWrapper.get(), schemeId, collectionId, version, data);
                    return removedObjectLocation.pointer();
                } finally {
                    this.pageBuffer.release(page);
                }
            }

        }

        DBObject dbObject = null;
        Page page = null;

        Optional<Page> optionalLastPage = this.getBufferedLastPage();
        if (optionalLastPage.isPresent()) {
            page = optionalLastPage.get();
            try {
                Optional<DBObject> optionalDBObjectWrapper = page.getEmptyDBObjectWrapper(data.length);
                if (optionalDBObjectWrapper.isPresent()) {
                    dbObject = optionalDBObjectWrapper.get();
                }
            } catch (InvalidDBObjectWrapper e) {
                this.pageBuffer.release(page);
                throw e;
            }
        }

        if (dbObject == null){
            page = this.getBufferedNewPage();
            try {
                dbObject = page.getEmptyDBObjectWrapper(data.length).get();
            } catch (InvalidDBObjectWrapper e) {
                this.pageBuffer.release(page);
                throw e;
            }
        }

        try {
            this.store(dbObject, schemeId, collectionId, version, data);
            return new Pointer(
                    Pointer.TYPE_DATA,
                    ((long) page.getPageNumber() * this.engineConfig.getDbPageSize()) + dbObject.getBegin(),
                    page.getChunk()
            );
        } finally {
            this.pageBuffer.release(page);
        }
    }

    private void store(DBObject dbObject, int schemeId, int collectionId, int version, byte[] data) throws InternalOperationException {
        dbObject.activate();
        dbObject.setSchemeId(schemeId);
        dbObject.modifyData(data);
        dbObject.setCollectionId(collectionId);
        dbObject.setVersion(version);
        this.commitPage(dbObject.getPage());
    }

    public void update(Pointer pointer, DBObjectUpdateConsumer<DBObject> dbObjectConsumer) throws InternalOperationException {
        PageBuffer.PageTitle pageTitle = new PageBuffer.PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.engineConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            Optional<DBObject> optionalDBObjectWrapper = page.getDBObjectFromPool(
                (int) (pointer.getPosition() % this.engineConfig.getDbPageSize())
            );


            if (optionalDBObjectWrapper.isEmpty()) {}  // Todo

            DBObject dbObject = optionalDBObjectWrapper.get();
            dbObjectConsumer.accept(dbObject);

            this.commitPage(dbObject.getPage());
        } finally {
            this.pageBuffer.release(page);
        }

    }

    @Override
    public void update(Pointer pointer, byte[] bytes) throws InternalOperationException {
        PageBuffer.PageTitle pageTitle = new PageBuffer.PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.engineConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            Optional<DBObject> optionalDBObjectWrapper;

            optionalDBObjectWrapper = page.getDBObjectFromPool(
                    (int) (pointer.getPosition() % this.engineConfig.getDbPageSize())
            );

            if (optionalDBObjectWrapper.isEmpty()) {}  // Todo

            DBObject dbObject = optionalDBObjectWrapper.get();
            dbObject.modifyData(bytes);
            this.commitPage(dbObject.getPage());
        } finally {
            this.pageBuffer.release(page);
        }
    }

    public Optional<DBObject> select(Pointer pointer) throws InternalOperationException {
        PageBuffer.PageTitle pageTitle = new PageBuffer.PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.engineConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            Optional<DBObject> optional = page.getDBObjectFromPool(
                    (int) (pointer.getPosition() % this.engineConfig.getDbPageSize())
            );
            if (optional.isPresent()) {
                return Optional.of(new MutableDBObjectDecorator(optional.get()));
            }
            return Optional.empty();
        } finally {
            this.pageBuffer.release(page);
        }
    }

    public void remove(Pointer pointer) throws InternalOperationException {
        PageBuffer.PageTitle pageTitle = new PageBuffer.PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.engineConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            Optional<DBObject> optional = page.getDBObjectFromPool(
                    (int) (pointer.getPosition() % this.engineConfig.getDbPageSize())
            );
            if (optional.isPresent()) {
                DBObject dbObject = optional.get();
                dbObject.deactivate();
                int dbObjectLength = dbObject.getLength();
                int offset = (int) (pointer.getPosition() % this.engineConfig.getDbPageSize());
                page.cleanPool(offset, dbObjectLength);
                this.commitPage(page);

                this.removedObjectsTracer.add(
                        new RemovedObjectsTracer.RemovedObjectLocation(
                                pointer,
                                dbObjectLength
                        )
                );
            }
        } finally {
            this.pageBuffer.release(page);
        }
    }



    /****** Helpers ******/


    private void commitPage(Page page) throws InternalOperationException {
        Path path = getDBFileName(page.getChunk());
        AsynchronousFileChannel fileChannel = this.fileHandlerPool.getFileChannel(path, 100, TimeUnit.SECONDS);

        // Todo: could return future instead maybe? Or just queue for submission?
        //       For transactions we'd need something similar to FileSessionIO!
        //       In such case, releasing the page from buffer should happen after write is completed
        //       Maybe would be a good idea to do that (last line) here now too?  Temp Answer: No, we'd release twice for a single acquire
        //       Call backs could help in that case (CompletableFuture has such functionality)
        try {
            FileUtils.write(fileChannel, (long) page.getPageNumber() * this.engineConfig.getDbPageSize(), page.getData()).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new InternalOperationException(EM_FILE_WRITE, e);
        } finally {
            this.fileHandlerPool.releaseFileChannel(path, 100, TimeUnit.SECONDS); // Todo
        }

    }

    // Factory function to be used only in the buffer
    private Page pageFactory(PageBuffer.PageTitle pageTitle) throws InternalOperationException {
        try {
            Path path = getDBFileName(pageTitle.chunk());

            AsynchronousFileChannel fileChannel = fileHandlerPool.getFileChannel(
                    path,
                    100,        // Todo
                    TimeUnit.SECONDS
            );
            int size = this.engineConfig.getDbPageSize();
            int offset = pageTitle.pageNumber() * size;

            byte[] data = FileUtils.readBytes(fileChannel, offset, size).get();

            // Note: Apparently this if statement may not be useful ever!
            if (data.length == 0) {
                FileUtils.allocate(fileChannel, offset, size).get();
                data = FileUtils.readBytes(fileChannel, offset, size).get();
            }

            fileHandlerPool.releaseFileChannel(path, 100, TimeUnit.SECONDS);
            return new Page(
                    pageTitle.pageNumber(),
                    size,
                    pageTitle.chunk(),
                    data
            );
        } catch (InterruptedException | IOException | ExecutionException e) {
            throw new InternalOperationException(ErrorMessage.EM_FILE_ALLOCATION, e);
        }
    }

    public Path getDBFileName(int chunk){
        return Path.of(
                this.engineConfig.getBaseDBPath(),
                String.format("testudo_%d.db.bin", chunk)
        );
    }

    private Page getBufferedPage(int chunk, long offset) throws InternalOperationException {
        int pageNumber = (int) (offset / this.engineConfig.getDbPageSize());
        PageBuffer.PageTitle pageTitle = new PageBuffer.PageTitle(chunk, pageNumber);
        return this.pageBuffer.acquire(pageTitle);
    }

    private synchronized Page getBufferedNewPage() throws InternalOperationException {
        PageBuffer.PageTitle lastPageTitle = this.lastPageTitle;  // This surely exists since getBufferedLastPage was called first

        int chunk = 0;
        int pageNumber = 0;

        if (lastPageTitle != null){
            chunk = lastPageTitle.chunk();
            pageNumber = lastPageTitle.pageNumber() + 1;
        }

        if (this.engineConfig.getDbPageMaxFileSize() != EngineConfig.UNLIMITED_FILE_SIZE && (long) pageNumber * this.engineConfig.getDbPageSize() > this.engineConfig.getDbPageMaxFileSize()) {
            chunk += 1;
        }
        this.generateNewEmptyPage(chunk);

        this.lastPageTitle = new PageBuffer.PageTitle(chunk, pageNumber);
        return this.pageBuffer.acquire(this.lastPageTitle);
    }

    private synchronized void generateNewEmptyPage(int chunk) throws InternalOperationException {
        int size = this.engineConfig.getDbPageSize();
        Path path = getDBFileName(chunk);
        AsynchronousFileChannel fileChannel;
        try {
            fileChannel = this.fileHandlerPool.getFileChannel(path, 100, TimeUnit.SECONDS);// Todo
        } catch (InternalOperationException e) {
            throw new InternalOperationException(EM_FILEHANDLER_POOL, e);
        }
        try {
            FileUtils.allocate(fileChannel, size).get();
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new InternalOperationException(EM_FILE_ALLOCATION, e);
        } finally {
            this.fileHandlerPool.releaseFileChannel(path, 100, TimeUnit.SECONDS); // Todo
        }
    }

    private Optional<Page> getBufferedLastPage() throws InternalOperationException {
        if (this.lastPageTitle != null){
            return Optional.of(this.pageBuffer.acquire(this.lastPageTitle));
        }

        int lastChunk = -1;

        while (
                Files.exists(
                        getDBFileName(lastChunk + 1)
                )
        ){
            lastChunk++;
        }

        if (lastChunk == -1){
            return Optional.empty();
        }

        synchronized (this){
            try {
                AsynchronousFileChannel fileChannel = fileHandlerPool.getFileChannel(getDBFileName(lastChunk), 100, TimeUnit.SECONDS);// Todo
                long fileSize = fileChannel.size();
                long size = fileSize == 0 ? 0 : fileSize - 1;
                int pageNumber = (int) (size / this.engineConfig.getDbPageSize());
                fileHandlerPool.releaseFileChannel(getDBFileName(lastChunk), 100, TimeUnit.SECONDS);  // Todo

                this.lastPageTitle = new PageBuffer.PageTitle(lastChunk, pageNumber);

                return Optional.of(this.pageBuffer.acquire(this.lastPageTitle));
            } catch (IOException e) {
                throw new InternalOperationException(EM_FILEHANDLER_POOL, e);
            }
        }
    }

    @Override
    public void close() {
        this.pageBuffer.releaseAll();
    }
}
