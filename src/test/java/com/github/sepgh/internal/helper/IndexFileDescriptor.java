package com.github.sepgh.internal.helper;

import com.github.sepgh.internal.EngineConfig;
import com.github.sepgh.internal.storage.header.HeaderManager;
import com.github.sepgh.internal.tree.Pointer;
import com.github.sepgh.internal.tree.node.BaseTreeNode;
import com.github.sepgh.internal.tree.node.InternalTreeNode;
import com.github.sepgh.internal.tree.node.LeafTreeNode;
import com.github.sepgh.internal.utils.FileUtils;
import com.google.common.hash.HashCode;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Iterator;
import java.util.Map;
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

            BaseTreeNode baseTreeNode = BaseTreeNode.fromBytes(bytes);
            if (baseTreeNode.isLeaf())
                this.printLeafNode((LeafTreeNode) baseTreeNode, offset);
            else
                this.printInternalNode((InternalTreeNode) baseTreeNode, offset);
        }

    }

    private void printInternalNode(InternalTreeNode node, int offset) {
        System.out.println();
        System.out.println(HashCode.fromBytes(node.toBytes()));
        System.out.printf("Offset: %d%n", offset);
        System.out.printf("Node Header:  root(%s) [internal] %n", node.isRoot() ? "T" : "F");
        System.out.println("Keys:" + node.keyList());
        System.out.println("Children:" + node.childrenList());
        System.out.println("===========================");
    }

    private void printEmptyNode(int offset) {
        System.out.println();
        System.out.printf("Offset: %d%n", offset);
        System.out.println("EMPTY");
        System.out.println("===========================");
    }

    private void printLeafNode(LeafTreeNode node, int offset){
        System.out.println();
        System.out.println(HashCode.fromBytes(node.toBytes()));
        System.out.printf("Offset: %d%n", offset);
        System.out.printf("Node Header:  root(%s) [leaf] %n", node.isRoot() ? "T" : "F");
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<Map.Entry<Long, Pointer>> entryIterator = node.keyValues();
        while (entryIterator.hasNext()) {
            Map.Entry<Long, Pointer> next = entryIterator.next();
            stringBuilder
                    .append("\t")
                    .append("K: ")
                    .append(next.getKey())
                    .append(" V: ")
                    .append(next.getValue())
                    .append(" OFFSET: TBD")
                    .append("\n")
            ;
        }
        System.out.println("Key Values:");
        System.out.println(stringBuilder);
        System.out.println("===========================");
    }


}
