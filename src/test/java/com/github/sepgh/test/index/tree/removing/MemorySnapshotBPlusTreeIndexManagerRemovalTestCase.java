package com.github.sepgh.test.index.tree.removing;

import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.IndexManager;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.index.tree.node.data.LongImmutableBinaryObjectWrapper;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.session.MemorySnapshotIndexIOSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.ExecutionException;


/* Identical copy of BPlusTreeIndexManagerRemovalTestCase */
public class MemorySnapshotBPlusTreeIndexManagerRemovalTestCase extends BPlusTreeIndexManagerRemovalTestCase {

    @Override
    protected IndexManager<Long, Pointer> getIndexManager(IndexStorageManager indexStorageManager) {
        return new ClusterBPlusTreeIndexManager<>(1, degree, indexStorageManager, MemorySnapshotIndexIOSession.Factory.getInstance(), new LongImmutableBinaryObjectWrapper());
    }

    @Test
    @Timeout(2)
    @Override
    public void testRemovingLeftToRight() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        super.testRemovingLeftToRight();
    }


    @Test
    @Timeout(2)
    @Override
    public void testRemovingRightToLeft() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        super.testRemovingRightToLeft();
    }

    @Test
    @Timeout(2)
    @Override
    public void testRemovingRoot() throws IOException, ExecutionException, InterruptedException, ImmutableBinaryObjectWrapper.InvalidBinaryObjectWrapperValue, IndexExistsException, InternalOperationException {
        super.testRemovingRoot();
    }


    @Test
    @Timeout(2)
    @Override
    public void testRemovingLeftToRightAsync() throws IOException, ExecutionException, InterruptedException, InternalOperationException {
        super.testRemovingLeftToRightAsync();
    }
}
