package com.github.sepgh.test.index.tree.removing;

import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.IndexExistsException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.index.tree.node.cluster.ClusterBPlusTreeUniqueTreeIndexManager;
import com.github.sepgh.testudo.storage.index.IndexStorageManager;
import com.github.sepgh.testudo.storage.index.session.MemorySnapshotIndexIOSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.github.sepgh.test.TestParams.DEFAULT_INDEX_BINARY_OBJECT_FACTORY;


/* Identical copy of BPlusTreeIndexManagerRemovalTestCase */
public class MemorySnapshotBPlusTreeUniqueTreeIndexManagerRemovalTestCase extends BPlusTreeUniqueTreeIndexManagerRemovalTestCase {

    @Override
    protected UniqueTreeIndexManager<Long, Pointer> getIndexManager(IndexStorageManager indexStorageManager) {
        return new ClusterBPlusTreeUniqueTreeIndexManager<>(1, degree, indexStorageManager, MemorySnapshotIndexIOSession.Factory.getInstance(), DEFAULT_INDEX_BINARY_OBJECT_FACTORY.get());
    }

    @Test
    @Timeout(2)
    @Override
    public void testRemovingLeftToRight() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        super.testRemovingLeftToRight();
    }


    @Test
    @Timeout(2)
    @Override
    public void testRemovingRightToLeft() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        super.testRemovingRightToLeft();
    }

    @Test
    @Timeout(2)
    @Override
    public void testRemovingRoot() throws IOException, ExecutionException, InterruptedException, IndexExistsException, InternalOperationException {
        super.testRemovingRoot();
    }
}
