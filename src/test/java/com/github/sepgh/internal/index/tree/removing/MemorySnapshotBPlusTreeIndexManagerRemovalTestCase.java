package com.github.sepgh.internal.index.tree.removing;

import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.tree.BPlusTreeIndexManager;
import com.github.sepgh.internal.index.tree.node.cluster.ClusterIdentifier;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.session.MemorySnapshotIndexIOSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.ExecutionException;


/* Identical copy of BPlusTreeIndexManagerRemovalTestCase */
public class MemorySnapshotBPlusTreeIndexManagerRemovalTestCase extends BPlusTreeIndexManagerRemovalTestCase {

    @Override
    protected IndexManager<Long> getIndexManager(IndexStorageManager indexStorageManager) {
        return new BPlusTreeIndexManager<>(degree, indexStorageManager, MemorySnapshotIndexIOSession.Factory.getInstance(), ClusterIdentifier.LONG);
    }

    @Test
    @Timeout(2)
    @Override
    public void testRemovingLeftToRight() throws IOException, ExecutionException, InterruptedException {
        super.testRemovingLeftToRight();
    }


    @Test
    @Timeout(2)
    @Override
    public void testRemovingRightToLeft() throws IOException, ExecutionException, InterruptedException {
        super.testRemovingRightToLeft();
    }

    @Test
    @Timeout(2)
    @Override
    public void testRemovingRoot() throws IOException, ExecutionException, InterruptedException {
        super.testRemovingRoot();
    }


    @Test
    @Timeout(2)
    @Override
    public void testRemovingLeftToRightAsync() throws IOException, ExecutionException, InterruptedException {
        super.testRemovingLeftToRightAsync();
    }
}
