package com.github.sepgh.test.storage.db;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.EngineConfig;
import com.github.sepgh.testudo.exception.VerificationException;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.AbstractLeafTreeNode;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DiskPageDatabaseStorageManager;
import com.github.sepgh.testudo.storage.db.Page;
import com.github.sepgh.testudo.storage.db.PageBuffer;
import com.github.sepgh.testudo.storage.pool.FileHandler;
import com.github.sepgh.testudo.storage.pool.UnlimitedFileHandlerPool;
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

    private DiskPageDatabaseStorageManager diskPageDatabaseStorageManager;
    private Path dbPath;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory("TEST_DatabaseStorageManagerTestCase");
        EngineConfig engineConfig = EngineConfig.builder()
                .baseDBPath(this.dbPath.toString())
                .build();
        this.diskPageDatabaseStorageManager = new DiskPageDatabaseStorageManager(
                engineConfig,
                new UnlimitedFileHandlerPool(
                        FileHandler.SingletonFileHandlerFactory.getInstance()
                )
        );
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    @Test
    public void test_canStoreObject() throws IOException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = this.diskPageDatabaseStorageManager.store(17, 1, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        // Making sure page wrapper is no longer held in referenced pages
        PageBuffer pageBuffer = this.diskPageDatabaseStorageManager.getPageBuffer();
        Field referencedWrappers = PageBuffer.class.getDeclaredField("referencedWrappers");
        referencedWrappers.setAccessible(true);
        Map<PageBuffer.PageTitle, PageBuffer.PageWrapper> referencedPageWrappers = (Map<PageBuffer.PageTitle, PageBuffer.PageWrapper>) referencedWrappers.get(pageBuffer);
        Assertions.assertTrue(referencedPageWrappers.isEmpty());
    }

    @Test
    public void test_canStoreAndReadObject() throws IOException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = this.diskPageDatabaseStorageManager.store(17,1, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        Optional<DBObject> optionalDBObjectWrapper = this.diskPageDatabaseStorageManager.select(pointer);
        Assertions.assertTrue(optionalDBObjectWrapper.isPresent());

        DBObject dbObject = optionalDBObjectWrapper.get();
        Assertions.assertTrue(dbObject.isAlive());
        Assertions.assertEquals(17, dbObject.getCollectionId());
        String string = new String(dbObject.getData(), StandardCharsets.UTF_8);
        Assertions.assertEquals("Test", string);
    }

    @Test
    public void test_canStoreUpdateAndReadObject() throws IOException, ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = this.diskPageDatabaseStorageManager.store(17,1, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        this.diskPageDatabaseStorageManager.update(pointer, dbObject -> {
            try {
                dbObject.modifyData("Nest".getBytes(StandardCharsets.UTF_8));
            } catch (VerificationException.InvalidDBObjectWrapper e) {
                throw new RuntimeException(e);
            }
        });

        Optional<DBObject> optionalDBObjectWrapper = this.diskPageDatabaseStorageManager.select(pointer);
        Assertions.assertTrue(optionalDBObjectWrapper.isPresent());

        DBObject dbObject = optionalDBObjectWrapper.get();
        Assertions.assertTrue(dbObject.isAlive());
        Assertions.assertEquals(17, dbObject.getCollectionId());
        String string = new String(dbObject.getData(), StandardCharsets.UTF_8);
        Assertions.assertEquals("Nest", string);
    }

    @Test
    public void test_canStoreDeleteAndReuseObject() throws IOException, ExecutionException, InterruptedException {
        byte[] data = "Test".getBytes(StandardCharsets.UTF_8);
        Pointer pointer = this.diskPageDatabaseStorageManager.store(17, 1, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());

        this.diskPageDatabaseStorageManager.remove(pointer);
        pointer = this.diskPageDatabaseStorageManager.store(17, 1, data);
        Assertions.assertNotNull(pointer);
        Assertions.assertEquals(0, pointer.getChunk());
        Assertions.assertEquals(Page.META_BYTES, pointer.getPosition());
    }

    @Test
    public void test_canStoreDeleteAndReuseMultipleObjects() throws IOException, ExecutionException, InterruptedException {
        List<String> inputs = Arrays.asList("10", "100", "1000", "10000");
        List<Pointer> pointers = new ArrayList<>();

        for (int i = 0; i < inputs.size(); i++) {
            byte[] data = inputs.get(i).getBytes(StandardCharsets.UTF_8);
            Pointer pointer = this.diskPageDatabaseStorageManager.store(17, 1, data);
            Assertions.assertNotNull(pointer);
            pointers.add(i, pointer);
        }

        for (int i = 0; i < inputs.size(); i++){
            Pointer pointer = pointers.get(i);
            Optional<DBObject> optionalDBObjectWrapper = this.diskPageDatabaseStorageManager.select(pointer);
            Assertions.assertTrue(optionalDBObjectWrapper.isPresent());
            Assertions.assertTrue(optionalDBObjectWrapper.get().isAlive());
            Assertions.assertEquals(inputs.get(i), new String(optionalDBObjectWrapper.get().getData(), StandardCharsets.UTF_8));
        }

        for (Pointer pointer : pointers) {
            this.diskPageDatabaseStorageManager.remove(pointer);
            Optional<DBObject> optionalDBObjectWrapper = this.diskPageDatabaseStorageManager.select(pointer);
            Assertions.assertTrue(optionalDBObjectWrapper.isPresent());
            Assertions.assertFalse(optionalDBObjectWrapper.get().isAlive());
        }


        for (int i = 0; i < inputs.size(); i++) {
            byte[] data = inputs.get(i).getBytes(StandardCharsets.UTF_8);
            Pointer pointer = this.diskPageDatabaseStorageManager.store(17, 1, data);
            Assertions.assertEquals(pointers.get(i), pointer);
        }

    }


    @Test
    public void test_multiThreadedInsertAndSelect() throws IOException, ExecutionException, InterruptedException {
        int cases = 20;

        ExecutorService executorService = Executors.newFixedThreadPool(cases);
        List<AbstractLeafTreeNode.KeyValue<String, Pointer>> keyValues = new CopyOnWriteArrayList<>();

        CountDownLatch countDownLatch = new CountDownLatch(cases);

        for (int i = 0; i < cases; i++) {
            executorService.submit(() -> {
                Random random = new Random();
                byte[] array = new byte[random.nextInt(100) + 1]; // length is bounded by 7
                random.nextBytes(array);
                String generatedString = new String(array, StandardCharsets.UTF_8);
                try {
                    byte[] generatedStringBytes = generatedString.getBytes(StandardCharsets.UTF_8);
                    Pointer pointer = diskPageDatabaseStorageManager.store(1, 1, generatedStringBytes);
                    keyValues.add(new AbstractLeafTreeNode.KeyValue<>(generatedString, pointer));
                } catch (IOException | InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }

        countDownLatch.await();

        for (AbstractLeafTreeNode.KeyValue<String, Pointer> keyValue : keyValues) {
            Optional<DBObject> dbObject = this.diskPageDatabaseStorageManager.select(keyValue.value());
            Assertions.assertTrue(dbObject.isPresent());
            Assertions.assertTrue(dbObject.get().isAlive());
            String value = new String(dbObject.get().getData(), StandardCharsets.UTF_8);
            Assertions.assertEquals(keyValue.key(), value);
        }


        executorService.shutdownNow();
    }

}
