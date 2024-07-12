package com.github.sepgh.testudo.storage.index;

import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.tree.node.data.PointerImmutableBinaryObjectWrapper;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BTreeSizeCalculator {
    private int degree;
    private int keySize;
    private int valueSize;

    public BTreeSizeCalculator setDegree(int degree) {
        this.degree = degree;
        return this;
    }

    public BTreeSizeCalculator setKeySize(int keySize) {
        this.keySize = keySize;
        return this;
    }

    public BTreeSizeCalculator setValueSize(int valueSize) {
        this.valueSize = valueSize;
        return this;
    }

    public int calculate(){
        int value = 1 + (degree * (keySize + valueSize)) + (2 * Pointer.BYTES);
        int i = value % 8;
        if (i == 0){
            return value;
        }
        return value + 8 - i;
    }

    public static int getClusteredBPlusTreeSize(int degree, int keySize){
        return new BTreeSizeCalculator(degree, keySize, PointerImmutableBinaryObjectWrapper.BYTES).calculate();
    }
}
