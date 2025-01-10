package com.github.sepgh.test.storage.db;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.ds.KeyValue;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.InvalidDBObjectWrapper;
import com.github.sepgh.testudo.storage.db.*;
import com.github.sepgh.testudo.storage.pool.FileHandlerPoolFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class DiskPageDatabaseStorageManagerTestCase {

    private EngineConfig engineConfig;
    private Path dbPath;
    private FileHandlerPoolFactory fileHandlerPoolFactory;


    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory("TEST_DatabaseStorageManagerTestCase");
        this.engineConfig = EngineConfig.builder()
                .baseDBPath(this.dbPath.toString())
                .build();
        this.fileHandlerPoolFactory = new FileHandlerPoolFactory.DefaultFileHandlerPoolFactory(engineConfig);
    }

    private DatabaseStorageManagerFactory getDatabaseStorageManagerFactory() {
        return new DatabaseStorageManagerFactory.DiskPageDatabaseStorageManagerFactory(engineConfig, fileHandlerPoolFactory);
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    @Test
    public void test_canStoreObject() throws IOException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException, InternalOperationException {
        DatabaseStorageManagerFactory databaseStorageManagerFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerFactory.getInstance();
        
        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = storageManager.store(-1, 17, 1, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        // Making sure page wrapper is no longer held in referenced pages
        PageBuffer pageBuffer = ((DiskPageDatabaseStorageManager) storageManager).getPageBuffer();
        Field referencedWrappers = PageBuffer.class.getDeclaredField("referencedWrappers");
        referencedWrappers.setAccessible(true);
        Map<PageBuffer.PageTitle, PageBuffer.PageWrapper> referencedPageWrappers = (Map<PageBuffer.PageTitle, PageBuffer.PageWrapper>) referencedWrappers.get(pageBuffer);
        Assertions.assertTrue(referencedPageWrappers.isEmpty());
    }

    @Test
    public void test_canChangePages() throws IOException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException, InternalOperationException {
        this.engineConfig.setDbPageSize(100);
        DatabaseStorageManagerFactory databaseStorageManagerFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerFactory.getInstance();

        byte[] data = new byte[60];
        Pointer pointer = storageManager.store(-1, 17, 1, data);
        Assertions.assertNotNull(pointer);

        pointer = storageManager.store(-1, 17, 1, data);
        Assertions.assertNotNull(pointer);

        pointer = storageManager.store(-1, 17, 1, data);
        Assertions.assertNotNull(pointer);

        pointer = storageManager.store(-1, 17, 1, data);
        Assertions.assertNotNull(pointer);

        pointer = storageManager.store(-1, 17, 1, data);
        Assertions.assertNotNull(pointer);
    }


    @Test
    public void test_canStoreAndReadObject() throws IOException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException, InternalOperationException {
        DatabaseStorageManagerFactory databaseStorageManagerFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerFactory.getInstance();

        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = storageManager.store(-1, 17,1, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        Optional<DBObject> optionalDBObjectWrapper = storageManager.select(pointer);
        Assertions.assertTrue(optionalDBObjectWrapper.isPresent());

        DBObject dbObject = optionalDBObjectWrapper.get();
        Assertions.assertTrue(dbObject.isAlive());
        Assertions.assertEquals(17, dbObject.getCollectionId());
        String string = new String(dbObject.getData(), StandardCharsets.UTF_8);
        Assertions.assertEquals("Test", string);
    }

    @Test
    public void test_canStoreUpdateAndReadObject() throws IOException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException, InternalOperationException {
        DatabaseStorageManagerFactory databaseStorageManagerFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerFactory.getInstance();

        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = storageManager.store(-1, 17,1, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        storageManager.update(pointer, dbObject -> {
            try {
                dbObject.modifyData("Nest".getBytes(StandardCharsets.UTF_8));
            } catch (InvalidDBObjectWrapper e) {
                throw new RuntimeException(e);
            }
        });

        Optional<DBObject> optionalDBObjectWrapper = storageManager.select(pointer);
        Assertions.assertTrue(optionalDBObjectWrapper.isPresent());

        DBObject dbObject = optionalDBObjectWrapper.get();
        Assertions.assertTrue(dbObject.isAlive());
        Assertions.assertEquals(17, dbObject.getCollectionId());
        String string = new String(dbObject.getData(), StandardCharsets.UTF_8);
        Assertions.assertEquals("Nest", string);
    }

    @Test
    public void test_canStoreDeleteAndReuseObject() throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        DatabaseStorageManagerFactory databaseStorageManagerFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerFactory.getInstance();

        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = storageManager.store(-1, 17, 1, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        storageManager.remove(pointer);
        pointer = storageManager.store(-1, 17, 1, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());
    }

    @Test
    public void test_canStoreDeleteAndReuseMultipleObjects() throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        DatabaseStorageManagerFactory databaseStorageManagerFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerFactory.getInstance();

        List<String> inputs = Arrays.asList("10", "100", "1000", "10000");
        List<Pointer> pointers = new ArrayList<>();

        for (int i = 0; i < inputs.size(); i++) {
            byte[] data = inputs.get(i).getBytes(StandardCharsets.UTF_8);
            Pointer pointer = storageManager.store(-1, 17, 1, data);
            Assertions.assertNotNull(pointer);
            pointers.add(i, pointer);
        }

        for (int i = 0; i < inputs.size(); i++){
            Pointer pointer = pointers.get(i);
            Optional<DBObject> optionalDBObjectWrapper = storageManager.select(pointer);
            Assertions.assertTrue(optionalDBObjectWrapper.isPresent());
            Assertions.assertTrue(optionalDBObjectWrapper.get().isAlive());
            Assertions.assertEquals(inputs.get(i), new String(optionalDBObjectWrapper.get().getData(), StandardCharsets.UTF_8));
        }

        for (Pointer pointer : pointers) {
            storageManager.remove(pointer);
            Optional<DBObject> optionalDBObjectWrapper = storageManager.select(pointer);
            Assertions.assertTrue(optionalDBObjectWrapper.isPresent());
            Assertions.assertFalse(optionalDBObjectWrapper.get().isAlive());
        }


        for (int i = 0; i < inputs.size(); i++) {
            byte[] data = inputs.get(i).getBytes(StandardCharsets.UTF_8);
            Pointer pointer = storageManager.store(-1, 17, 1, data);
            Assertions.assertEquals(pointers.get(i), pointer);
        }

    }

    @Test
    public void testRemovedObjectTracerSplitLength() throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        this.engineConfig.setIMROTMinLengthToSplit(100);
        this.engineConfig.setDbPageSize(100000);

        DatabaseStorageManagerFactory databaseStorageManagerFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerFactory.getInstance();

        Pointer pointer1 = storageManager.store(-1, 1, 1, new byte[1000]);
        Pointer pointer2 = storageManager.store(-1, 1, 1, new byte[1000]);
        Pointer pointer3 = storageManager.store(-1, 1, 1, new byte[1000]);

        storageManager.remove(pointer2);
        Pointer pointer4 = storageManager.store(-1, 1, 1, new byte[1000]);

        Assertions.assertEquals(pointer2, pointer4);
        storageManager.remove(pointer4);

        pointer4 = storageManager.store(-1, 1, 1, new byte[100]);
        Assertions.assertEquals(pointer2, pointer4);

        List<RemovedObjectsTracer.RemovedObjectLocation> removedObjectLocations = ((DiskPageDatabaseStorageManager) storageManager).getRemovedObjectsTracer().getRemovedObjectLocations();
        Assertions.assertEquals(1, removedObjectLocations.size());
        

        Pointer pointer5 = storageManager.store(-1, 1, 1, new byte[100]);
        Assertions.assertEquals(
                pointer4.getPosition() + DBObject.getWrappedSize(100),
                pointer5.getPosition()
        );

    }


    @Test
    public void test_multiThreadedInsertAndSelect() throws InternalOperationException, InterruptedException {
        DatabaseStorageManagerFactory databaseStorageManagerFactory = getDatabaseStorageManagerFactory();
        DatabaseStorageManager storageManager = databaseStorageManagerFactory.getInstance();

        int cases = 20;

        ExecutorService executorService = Executors.newFixedThreadPool(cases);
        List<KeyValue<String, Pointer>> keyValues = new CopyOnWriteArrayList<>();

        CountDownLatch countDownLatch = new CountDownLatch(cases);

        for (int i = 0; i < cases; i++) {
            executorService.submit(() -> {
                Random random = new Random();
                byte[] array = new byte[random.nextInt(100) + 1]; // length is bounded by 7
                random.nextBytes(array);
                String generatedString = new String(array, StandardCharsets.UTF_8);
                try {
                    byte[] generatedStringBytes = generatedString.getBytes(StandardCharsets.UTF_8);
                    Pointer pointer = storageManager.store(-1, 1, 1, generatedStringBytes);
                    keyValues.add(new KeyValue<>(generatedString, pointer));
                } catch (InternalOperationException e) {
                    throw new RuntimeException(e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }

        countDownLatch.await();

        for (KeyValue<String, Pointer> keyValue : keyValues) {
            Optional<DBObject> dbObject = storageManager.select(keyValue.value());
            Assertions.assertTrue(dbObject.isPresent());
            Assertions.assertTrue(dbObject.get().isAlive());
            String value = new String(dbObject.get().getData(), StandardCharsets.UTF_8);
            Assertions.assertEquals(keyValue.key(), value);
        }


        executorService.shutdownNow();
    }

}
