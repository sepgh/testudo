package com.github.sepgh.internal.index.tree.removing;

import com.github.sepgh.internal.index.IndexManager;
import com.github.sepgh.internal.index.tree.BPlusTreeIndexManager;
import com.github.sepgh.internal.storage.IndexStorageManager;
import com.github.sepgh.internal.storage.session.MemorySnapshotIndexIOSession;


/* Identical copy of BPlusTreeIndexManagerRemovalTestCase */
public class MemorySnapshotBPlusTreeIndexManagerRemovalTestCase extends BPlusTreeIndexManagerRemovalTestCase {

    @Override
    protected IndexManager getIndexManager(IndexStorageManager indexStorageManager) {
        return new BPlusTreeIndexManager(degree, indexStorageManager, MemorySnapshotIndexIOSession.Factory.getInstance());
    }
}
