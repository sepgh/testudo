package com.github.sepgh.testudo.ds;

public record KeyValue<K extends Comparable<K>, V>(K key, V value) implements Comparable<KeyValue<K, V>> {

    @Override
    public int compareTo(KeyValue<K, V> o) {
        return this.key.compareTo(o.key);
    }
}
