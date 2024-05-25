package com.github.sepgh.internal.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class CollectionUtils {

    public static <T> int indexToInsert(List<? extends T> list, T itemToBeInserted, Comparator<? super T> comparator){
        int i = Collections.binarySearch(list, itemToBeInserted, comparator);
        if (i >= 0){
            throw new RuntimeException("CollectionUtils.indexToInsert() cant be called on a list that already contains the item!");
        }

        return (i * -1) - 1;
    }

    public static <T extends Comparable<T>> int indexToInsert(List<? extends T> list, T itemToBeInserted){
        int i = Collections.binarySearch(list, itemToBeInserted);
        if (i >= 0){
            throw new RuntimeException("CollectionUtils.indexToInsert() cant be called on a list that already contains the item!");
        }

        return (i * -1) - 1;
    }

}
