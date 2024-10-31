package com.github.sepgh.test.index;

import com.github.sepgh.test.TestParams;
import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.index.AscendingBinaryListIterator;
import com.github.sepgh.testudo.index.BinaryList;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.serialization.IntegerSerializer;
import com.google.common.primitives.Ints;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ListIterator;

public class AscendingBinaryListIteratorTestCase {
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

        byte[] data = new byte[BinaryList.META_SIZE + factory.size() * 3];
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
                BinaryList.META_SIZE,
                factory.size()
        );
        System.arraycopy(
                factory.create(2).getBytes(),
                0,
                data,
                BinaryList.META_SIZE + factory.size(),
                factory.size()
        );
        System.arraycopy(
                factory.create(3).getBytes(),
                0,
                data,
                BinaryList.META_SIZE + 2 * factory.size(),
                factory.size()
        );

        ListIterator<Integer> ascendingBinaryListIterator = new BinaryList<>(
                this.engineConfig,
                factory,
                data
        ).getIterator(Order.ASC);


        Assertions.assertTrue(ascendingBinaryListIterator.hasNext());
        Assertions.assertEquals(1, ascendingBinaryListIterator.next());
        Assertions.assertTrue(ascendingBinaryListIterator.hasNext());
        Assertions.assertEquals(2, ascendingBinaryListIterator.next());
        Assertions.assertTrue(ascendingBinaryListIterator.hasNext());
        Assertions.assertEquals(3, ascendingBinaryListIterator.next());
        Assertions.assertFalse(ascendingBinaryListIterator.hasNext());

        Assertions.assertTrue(ascendingBinaryListIterator.hasPrevious());
        Assertions.assertEquals(3, ascendingBinaryListIterator.previous());
        Assertions.assertTrue(ascendingBinaryListIterator.hasPrevious());
        Assertions.assertEquals(2, ascendingBinaryListIterator.previous());
        Assertions.assertTrue(ascendingBinaryListIterator.hasPrevious());
        Assertions.assertEquals(1, ascendingBinaryListIterator.previous());
        Assertions.assertFalse(ascendingBinaryListIterator.hasPrevious());

        Assertions.assertTrue(ascendingBinaryListIterator.hasNext());
        Assertions.assertEquals(1, ascendingBinaryListIterator.next());

    }


    @Test
    public void test_remove() throws Exception {
        IndexBinaryObjectFactory<Integer> factory = new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD);

        byte[] data = new byte[BinaryList.META_SIZE + factory.size() * 3];
        System.arraycopy(
                Ints.toByteArray(2),
                0,
                data,
                0,
                BinaryList.META_SIZE
        );
        System.arraycopy(
                factory.create(1).getBytes(),
                0,
                data,
                BinaryList.META_SIZE,
                factory.size()
        );
        System.arraycopy(
                factory.create(2).getBytes(),
                0,
                data,
                BinaryList.META_SIZE + factory.size(),
                factory.size()
        );
        System.arraycopy(
                factory.create(3).getBytes(),
                0,
                data,
                BinaryList.META_SIZE + factory.size() * 2,
                factory.size()
        );

        BinaryList<Integer> binaryList = new BinaryList<>(
                this.engineConfig,
                factory,
                data
        );

        Assertions.assertTrue(binaryList.remove(3));

        ListIterator<Integer> ascendingBinaryListIterator = binaryList.getIterator(Order.ASC);

        Assertions.assertTrue(ascendingBinaryListIterator.hasNext());
        Assertions.assertEquals(1, ascendingBinaryListIterator.next());
        Assertions.assertTrue(ascendingBinaryListIterator.hasNext());
        Assertions.assertEquals(2, ascendingBinaryListIterator.next());
        Assertions.assertFalse(ascendingBinaryListIterator.hasNext());


        Assertions.assertFalse(binaryList.remove(10));
        Assertions.assertTrue(binaryList.remove(2));

        ascendingBinaryListIterator = binaryList.getIterator(Order.ASC);

        Assertions.assertTrue(ascendingBinaryListIterator.hasNext());
        Assertions.assertEquals(1, ascendingBinaryListIterator.next());
        Assertions.assertFalse(ascendingBinaryListIterator.hasNext());
    }


    @Test
    public void test_addNew() throws Exception {
        IndexBinaryObjectFactory<Integer> factory = new IntegerSerializer().getIndexBinaryObjectFactory(TestParams.FAKE_FIELD);

        byte[] data = new byte[BinaryList.META_SIZE + factory.size() * 3];
        System.arraycopy(
                Ints.toByteArray(1),
                0,
                data,
                0,
                BinaryList.META_SIZE
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

        BinaryList<Integer> binaryList = new BinaryList<>(
                this.engineConfig,
                factory,
                data
        );

        binaryList.addNew(3);

        ListIterator<Integer> ascendingBinaryListIterator = binaryList.getIterator(Order.ASC);

        Assertions.assertTrue(ascendingBinaryListIterator.hasNext());
        Assertions.assertEquals(1, ascendingBinaryListIterator.next());
        Assertions.assertTrue(ascendingBinaryListIterator.hasNext());
        Assertions.assertEquals(2, ascendingBinaryListIterator.next());
        Assertions.assertTrue(ascendingBinaryListIterator.hasNext());
        Assertions.assertEquals(3, ascendingBinaryListIterator.next());
        Assertions.assertFalse(ascendingBinaryListIterator.hasNext());

        Assertions.assertThrows(RuntimeException.class, () -> binaryList.addNew(4)); // Todo: change exc

    }
}
