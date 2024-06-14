package com.github.sepgh.internal.index.tree.node;

import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.data.ImmutableBinaryObjectWrapper;
import com.github.sepgh.internal.storage.IndexStorageManager;

import static com.github.sepgh.internal.index.tree.node.AbstractTreeNode.TYPE_LEAF_NODE_BIT;

public interface NodeFactory<K extends Comparable<K>> {
    AbstractTreeNode<K> fromBytes(byte[] bytes);
    default AbstractTreeNode<K> fromBytes(byte[] bytes, Pointer pointer) {
        AbstractTreeNode<K> treeNode = this.fromBytes(bytes);
        treeNode.setPointer(pointer);
        return treeNode;
    }

    default AbstractTreeNode<K> fromNodeData(IndexStorageManager.NodeData nodeData){
        return this.fromBytes(nodeData.bytes(), nodeData.pointer());
    }
    AbstractTreeNode<K> fromBytes(byte[] emptyNode, AbstractTreeNode.Type type);

    class ClusterNodeFactory<K extends Comparable<K>> implements NodeFactory<K> {

        private final ImmutableBinaryObjectWrapper<K> keyImmutableBinaryObjectWrapper;

        public ClusterNodeFactory(ImmutableBinaryObjectWrapper<K> keyImmutableBinaryObjectWrapper) {
            this.keyImmutableBinaryObjectWrapper = keyImmutableBinaryObjectWrapper;
        }

        @Override
        public AbstractTreeNode<K> fromBytes(byte[] bytes) {
            if ((bytes[0] & TYPE_LEAF_NODE_BIT) == TYPE_LEAF_NODE_BIT)
                return new LeafClusterTreeNode<>(bytes, keyImmutableBinaryObjectWrapper);
            return new InternalTreeNode<>(bytes, keyImmutableBinaryObjectWrapper);
        }

        @Override
        public AbstractTreeNode<K> fromBytes(byte[] bytes, AbstractTreeNode.Type type) {
            bytes[0] = type.getSign();
            if (type.equals(AbstractTreeNode.Type.LEAF))
                return new LeafClusterTreeNode<>(bytes, keyImmutableBinaryObjectWrapper);
            return new InternalTreeNode<>(bytes, keyImmutableBinaryObjectWrapper);
        }
    }

}
