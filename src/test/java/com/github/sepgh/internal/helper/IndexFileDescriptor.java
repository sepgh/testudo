package com.github.sepgh.internal.helper;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.index.Pointer;
import com.github.sepgh.internal.index.tree.node.cluster.BaseClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.InternalClusterTreeNode;
import com.github.sepgh.internal.index.tree.node.cluster.LeafClusterTreeNode;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.utils.FileUtils;
import com.google.common.hash.HashCode;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;


@AllArgsConstructor
public class IndexFileDescriptor {
    private final AsynchronousFileChannel asynchronousFileChannel;
    private final HeaderManager headerManager;
    private final EngineConfig engineConfig;

    public void describe() throws IOException, ExecutionException, InterruptedException {
        long paddingCounts = asynchronousFileChannel.size() / engineConfig.getPaddedSize();

        for (int i = 0; i < paddingCounts; i++){
            int offset = i * engineConfig.getPaddedSize();
            byte[] bytes = FileUtils.readBytes(this.asynchronousFileChannel, offset, engineConfig.getPaddedSize()).get();

            if (bytes.length == 0 || bytes[0] == 0){
                this.printEmptyNode(offset);
                continue;
            }

            BaseClusterTreeNode baseClusterTreeNode = BaseClusterTreeNode.fromBytes(bytes);
            if (baseClusterTreeNode.getType() == BaseClusterTreeNode.Type.LEAF)
                this.printLeafNode((LeafClusterTreeNode) baseClusterTreeNode, offset);
            else if (baseClusterTreeNode.getType() == BaseClusterTreeNode.Type.INTERNAL)
                this.printInternalNode((InternalClusterTreeNode) baseClusterTreeNode, offset);
            else
                System.out.println("Empty leaf?");
        }

    }

    private void printInternalNode(InternalClusterTreeNode node, int offset) {
        System.out.println();
        System.out.println(HashCode.fromBytes(node.toBytes()));
        System.out.printf("Offset: %d%n", offset);
        System.out.printf("Node Header:  root(%s) [internal] %n", node.isRoot() ? "T" : "F");
        System.out.println("Keys: " + node.getKeyList(engineConfig.getBTreeDegree() + 1));
        System.out.println("Children: ");
        for (Pointer pointer : node.getChildrenList()) {
            System.out.println("\t" + pointer.toString());
        }
        System.out.println();
        System.out.println("===========================");
    }

    private void printEmptyNode(int offset) {
        System.out.println();
        System.out.printf("Offset: %d%n", offset);
        System.out.println("EMPTY");
        System.out.println();
        System.out.println("===========================");
    }

    private void printLeafNode(LeafClusterTreeNode node, int offset){
        System.out.println();
        System.out.println(HashCode.fromBytes(node.toBytes()));
        System.out.printf("Offset: %d%n", offset);
        System.out.printf("Node Header:  root(%s) [leaf] %n", node.isRoot() ? "T" : "F");
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<LeafClusterTreeNode.KeyValue> entryIterator = node.getKeyValues(engineConfig.getBTreeDegree());
        while (entryIterator.hasNext()) {
            LeafClusterTreeNode.KeyValue next = entryIterator.next();
            stringBuilder
                    .append("\t")
                    .append("K: ")
                    .append(next.key())
                    .append(" V: ")
                    .append(next.value())
                    .append(" OFFSET: TBD")
                    .append("\n")
            ;
        }
        System.out.println("Key Values:");
        System.out.println(stringBuilder);
        System.out.println("Prev: " + node.getPreviousSiblingPointer(engineConfig.getBTreeDegree()));
        System.out.println("Next: " + node.getNextSiblingPointer(engineConfig.getBTreeDegree()));
        System.out.println("===========================");
    }


}
