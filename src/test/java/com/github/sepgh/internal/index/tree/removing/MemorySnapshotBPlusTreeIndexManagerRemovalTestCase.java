package com.github.sepgh.internal.index.tree.removing;

import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.internal.index.tree.node.data.BinaryObjectWrapper;
import com.github.sepgh.internal.index.tree.node.data.LongBinaryObjectWrapper;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.session.MemorySnapshotIndexIOSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.ExecutionException;


/* Identical copy of BPlusTreeIndexManagerRemovalTestCase */
public class MemorySnapshotBPlusTreeIndexManagerRemovalTestCase extends BPlusTreeIndexManagerRemovalTestCase {

    @Override
    protected IndexManager<Long, Pointer> getIndexManager(IndexStorageManager indexStorageManager) {
        return new ClusterBPlusTreeIndexManager<>(degree, indexStorageManager, MemorySnapshotIndexIOSession.Factory.getInstance(), new LongBinaryObjectWrapper());
    }

    @Test
    @Timeout(2)
    @Override
    public void testRemovingLeftToRight() throws IOException, ExecutionException, InterruptedException, BinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        super.testRemovingLeftToRight();
    }


    @Test
    @Timeout(2)
    @Override
    public void testRemovingRightToLeft() throws IOException, ExecutionException, InterruptedException, BinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        super.testRemovingRightToLeft();
    }

    @Test
    @Timeout(2)
    @Override
    public void testRemovingRoot() throws IOException, ExecutionException, InterruptedException, BinaryObjectWrapper.InvalidBinaryObjectWrapperValue {
        super.testRemovingRoot();
    }


    @Test
    @Timeout(2)
    @Override
    public void testRemovingLeftToRightAsync() throws IOException, ExecutionException, InterruptedException {
        super.testRemovingLeftToRightAsync();
    }
}
