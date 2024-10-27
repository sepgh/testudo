package com.github.sepgh.test.index;

import com.github.sepgh.test.TestParams;
import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.index.BinaryListIterator;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.serialization.IntegerSerializer;
import com.google.common.primitives.Ints;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class BinaryListIteratorTestCase {
    private Path dbPath;
    private EngineConfig engineConfig;

    @BeforeEach
    public void setUp() throws IOException {
        this.dbPath = Files.createTempDirectory("TEST_BinaryListIteratorTestCase");
        this.engineConfig = EngineConfig.builder()
                .baseDBPath(this.dbPath.toString())
                .dbPageSize(12 * Integer.BYTES)
                .bTreeDegree(10)
                .build();
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }

    @Test
    public void test_next_previous() throws Exception {
        IndexBinaryObjectFactory<Integer> factory = new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD);

        byte[] data = new byte[BinaryListIterator.META_SIZE + factory.size() * 3];
        System.arraycopy(
                Ints.toByteArray(2),
                0,
                data,
                0,
                factory.size()
        );
        System.arraycopy(
                factory.create(1).getBytes(),
                0,
                data,
                BinaryListIterator.META_SIZE,
                factory.size()
        );
        System.arraycopy(
                factory.create(2).getBytes(),
                0,
                data,
                BinaryListIterator.META_SIZE + factory.size(),
                factory.size()
        );
        System.arraycopy(
                factory.create(3).getBytes(),
                0,
                data,
                BinaryListIterator.META_SIZE + 2 * factory.size(),
                factory.size()
        );

        BinaryListIterator<Integer> binaryListIterator = new BinaryListIterator<>(
                this.engineConfig,
                factory,
                data
        );


        Assertions.assertTrue(binaryListIterator.hasNext());
        Assertions.assertEquals(1, binaryListIterator.next());
        Assertions.assertTrue(binaryListIterator.hasNext());
        Assertions.assertEquals(2, binaryListIterator.next());
        Assertions.assertTrue(binaryListIterator.hasNext());
        Assertions.assertEquals(3, binaryListIterator.next());
        Assertions.assertFalse(binaryListIterator.hasNext());

        Assertions.assertTrue(binaryListIterator.hasPrevious());
        Assertions.assertEquals(3, binaryListIterator.previous());
        Assertions.assertTrue(binaryListIterator.hasPrevious());
        Assertions.assertEquals(2, binaryListIterator.previous());
        Assertions.assertTrue(binaryListIterator.hasPrevious());
        Assertions.assertEquals(1, binaryListIterator.previous());
        Assertions.assertFalse(binaryListIterator.hasPrevious());

        Assertions.assertTrue(binaryListIterator.hasNext());
        Assertions.assertEquals(1, binaryListIterator.next());

    }


    @Test
    public void test_remove() throws Exception {
        IndexBinaryObjectFactory<Integer> factory = new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD);

        byte[] data = new byte[BinaryListIterator.META_SIZE + factory.size() * 3];
        System.arraycopy(
                Ints.toByteArray(2),
                0,
                data,
                0,
                BinaryListIterator.META_SIZE
        );
        System.arraycopy(
                factory.create(1).getBytes(),
                0,
                data,
                BinaryListIterator.META_SIZE,
                factory.size()
        );
        System.arraycopy(
                factory.create(2).getBytes(),
                0,
                data,
                BinaryListIterator.META_SIZE + factory.size(),
                factory.size()
        );
        System.arraycopy(
                factory.create(3).getBytes(),
                0,
                data,
                BinaryListIterator.META_SIZE + factory.size() * 2,
                factory.size()
        );

        BinaryListIterator<Integer> binaryListIterator = new BinaryListIterator<>(
                this.engineConfig,
                factory,
                data
        );


        Assertions.assertTrue(binaryListIterator.remove(3));

        Assertions.assertTrue(binaryListIterator.hasNext());
        Assertions.assertEquals(1, binaryListIterator.next());
        Assertions.assertTrue(binaryListIterator.hasNext());
        Assertions.assertEquals(2, binaryListIterator.next());
        Assertions.assertFalse(binaryListIterator.hasNext());


        Assertions.assertFalse(binaryListIterator.remove(10));
        Assertions.assertTrue(binaryListIterator.remove(2));
        binaryListIterator.resetCursor();

        Assertions.assertTrue(binaryListIterator.hasNext());
        Assertions.assertEquals(1, binaryListIterator.next());
        Assertions.assertFalse(binaryListIterator.hasNext());
    }


    @Test
    public void test_addNew() throws Exception {
        IndexBinaryObjectFactory<Integer> factory = new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD);

        byte[] data = new byte[BinaryListIterator.META_SIZE + factory.size() * 3];
        System.arraycopy(
                Ints.toByteArray(1),
                0,
                data,
                0,
                BinaryListIterator.META_SIZE
        );
        System.arraycopy(
                factory.create(1).getBytes(),
                0,
                data,
                factory.size(),
                factory.size()
        );
        System.arraycopy(
                factory.create(2).getBytes(),
                0,
                data,
                factory.size() * 2,
                factory.size()
        );

        BinaryListIterator<Integer> binaryListIterator = new BinaryListIterator<>(
                this.engineConfig,
                factory,
                data
        );

        binaryListIterator.addNew(3);
        Assertions.assertTrue(binaryListIterator.hasNext());
        Assertions.assertEquals(1, binaryListIterator.next());
        Assertions.assertTrue(binaryListIterator.hasNext());
        Assertions.assertEquals(2, binaryListIterator.next());
        Assertions.assertTrue(binaryListIterator.hasNext());
        Assertions.assertEquals(3, binaryListIterator.next());
        Assertions.assertFalse(binaryListIterator.hasNext());

        binaryListIterator.resetCursor();
        Assertions.assertThrows(RuntimeException.class, () -> binaryListIterator.addNew(4)); // Todo: change exc

    }
}
