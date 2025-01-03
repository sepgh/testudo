package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.VerificationException;
import com.github.sepgh.testudo.ds.Pointer;
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
import java.util.function.Consumer;


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


    public Pointer store(int schemeId, int collectionId, int version, byte[] data) throws IOException, InterruptedException, ExecutionException {
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

            try {
                Optional<DBObject> optionalDBObjectWrapper = page.getDBObjectFromPool(offset, data.length);
                if (optionalDBObjectWrapper.isPresent()) {
                    try {
                        this.store(optionalDBObjectWrapper.get(), schemeId, collectionId, version, data);
                        return removedObjectLocation.pointer();
                    } finally {
                        this.pageBuffer.release(page);
                    }
                }
            } catch (VerificationException.InvalidDBObjectWrapper e) {
                throw new RuntimeException(e);
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
            } catch (VerificationException.InvalidDBObjectWrapper e) {
                this.pageBuffer.release(page);
                throw new RuntimeException(e);
            }
        }

        if (dbObject == null){
            page = this.getBufferedNewPage();
            try {
                dbObject = page.getEmptyDBObjectWrapper(data.length).get();
            } catch (VerificationException.InvalidDBObjectWrapper e) {
                this.pageBuffer.release(page);
                throw new RuntimeException(e);
            }
        }

        try {
            this.store(dbObject, schemeId, collectionId, version, data);
            return new Pointer(
                    Pointer.TYPE_DATA,
                    ((long) page.getPageNumber() * this.engineConfig.getDbPageSize()) + dbObject.getBegin(),
                    page.getChunk()
            );
        } catch (VerificationException.InvalidDBObjectWrapper e) {
            throw new RuntimeException(e);
        } finally {
            this.pageBuffer.release(page);
        }
    }

    private void store(DBObject dbObject, int schemeId, int collectionId, int version, byte[] data) throws IOException, ExecutionException, InterruptedException, VerificationException.InvalidDBObjectWrapper {
        dbObject.activate();
        dbObject.setSchemeId(schemeId);
        dbObject.modifyData(data);
        dbObject.setCollectionId(collectionId);
        dbObject.setVersion(version);
        this.commitPage(dbObject.getPage());
    }

    public void update(Pointer pointer, Consumer<DBObject> dbObjectConsumer) throws IOException, ExecutionException, InterruptedException {
        PageBuffer.PageTitle pageTitle = new PageBuffer.PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.engineConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            Optional<DBObject> optionalDBObjectWrapper = null;
            try {
                optionalDBObjectWrapper = page.getDBObjectFromPool(
                        (int) (pointer.getPosition() % this.engineConfig.getDbPageSize())
                );
            } catch (VerificationException.InvalidDBObjectWrapper e) {
                throw new RuntimeException(e);
            }

            if (optionalDBObjectWrapper.isEmpty()) {}  // Todo

            DBObject dbObject = optionalDBObjectWrapper.get();
            dbObjectConsumer.accept(dbObject);

            this.commitPage(dbObject.getPage());
        } finally {
            this.pageBuffer.release(page);
        }

    }

    @Override
    public void update(Pointer pointer, byte[] bytes) throws IOException, ExecutionException, InterruptedException, VerificationException.InvalidDBObjectWrapper {
        PageBuffer.PageTitle pageTitle = new PageBuffer.PageTitle(pointer.getChunk(), (int) (pointer.getPosition() / this.engineConfig.getDbPageSize()));
        Page page = this.pageBuffer.acquire(pageTitle);

        try {
            Optional<DBObject> optionalDBObjectWrapper;
            try {
                optionalDBObjectWrapper = page.getDBObjectFromPool(
                        (int) (pointer.getPosition() % this.engineConfig.getDbPageSize())
                );
            } catch (VerificationException.InvalidDBObjectWrapper e) {
                throw new RuntimeException(e);
            }

            if (optionalDBObjectWrapper.isEmpty()) {}  // Todo

            DBObject dbObject = optionalDBObjectWrapper.get();
            dbObject.modifyData(bytes);
            this.commitPage(dbObject.getPage());
        } finally {
            this.pageBuffer.release(page);
        }
    }

    public Optional<DBObject> select(Pointer pointer){
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
        } catch (VerificationException.InvalidDBObjectWrapper e) {
            throw new RuntimeException(e);
        } finally {
            this.pageBuffer.release(page);
        }
    }

    public void remove(Pointer pointer) throws IOException, ExecutionException, InterruptedException {
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
        } catch (VerificationException.InvalidDBObjectWrapper e) {
            throw new RuntimeException(e);
        } finally {
            this.pageBuffer.release(page);
        }
    }



    /****** Helpers ******/


    private void commitPage(Page page) throws IOException, InterruptedException, ExecutionException {
        Path path = getDBFileName(page.getChunk());
        AsynchronousFileChannel fileChannel = this.fileHandlerPool.getFileChannel(path, 100, TimeUnit.SECONDS);// Todo

        // Todo: could return future instead maybe? Or just queue for submission?
        //       For transactions we'd need something similar to FileSessionIO!
        //       In such case, releasing the page from buffer should happen after write is completed
        //       Maybe would be a good idea to do that (last line) here now too?  Temp Answer: No, we'd release twice for a single acquire
        //       Call backs could help in that case (CompletableFuture has such functionality)
        try {
            FileUtils.write(fileChannel, (long) page.getPageNumber() * this.engineConfig.getDbPageSize(), page.getData()).get();
        } finally {
            this.fileHandlerPool.releaseFileChannel(path, 100, TimeUnit.SECONDS); // Todo
        }

    }

    // Factory function to be used only in the buffer
    private Page pageFactory(PageBuffer.PageTitle pageTitle){
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
            throw new RuntimeException(e);
        }
    }

    public Path getDBFileName(int chunk){
        return Path.of(
                this.engineConfig.getBaseDBPath(),
                String.format("testudo_%d.db.bin", chunk)
        );
    }

    private Page getBufferedPage(int chunk, long offset) {
        int pageNumber = (int) (offset / this.engineConfig.getDbPageSize());
        PageBuffer.PageTitle pageTitle = new PageBuffer.PageTitle(chunk, pageNumber);
        return this.pageBuffer.acquire(pageTitle);
    }

    private synchronized Page getBufferedNewPage() throws IOException, ExecutionException, InterruptedException {
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

    private synchronized void generateNewEmptyPage(int chunk) throws IOException, InterruptedException, ExecutionException {
        int size = this.engineConfig.getDbPageSize();
        Path path = getDBFileName(chunk);
        AsynchronousFileChannel fileChannel = this.fileHandlerPool.getFileChannel(path, 100, TimeUnit.SECONDS);// Todo
        try {
            FileUtils.allocate(fileChannel, size).get();
        } finally {
            this.fileHandlerPool.releaseFileChannel(path, 100, TimeUnit.SECONDS); // Todo
        }
    }

    private Optional<Page> getBufferedLastPage() throws IOException, InterruptedException {
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
            AsynchronousFileChannel fileChannel = fileHandlerPool.getFileChannel(getDBFileName(lastChunk), 100, TimeUnit.SECONDS);// Todo
            long fileSize = fileChannel.size();
            long size = fileSize == 0 ? 0 : fileSize - 1;
            int pageNumber = (int) (size / this.engineConfig.getDbPageSize());
            fileHandlerPool.releaseFileChannel(getDBFileName(lastChunk), 100, TimeUnit.SECONDS);  // Todo

            this.lastPageTitle = new PageBuffer.PageTitle(lastChunk, pageNumber);

            return Optional.of(this.pageBuffer.acquire(this.lastPageTitle));
        }
    }

}
