package com.github.sepgh.internal.index.tree.node.data;

public interface BinaryObjectWrapper<E extends Comparable<E>> {
    BinaryObjectWrapper<E> load(E e) throws InvalidBinaryObjectWrapperValue;
    BinaryObjectWrapper<E> load(byte[] bytes, int beginning);
    default BinaryObjectWrapper<E> load(byte[] bytes) {
        return this.load(bytes, 0);
    }
    E asObject();
    Class<E> getObjectClass();
    boolean hasValue();
    int size();
    byte[] getBytes();

    class InvalidBinaryObjectWrapperValue extends Exception {
        public InvalidBinaryObjectWrapperValue(Object object, Class<? extends BinaryObjectWrapper<?>> innerObjClass) {
            super(("%s is not a valid value for BinaryObjectWrapper of type " + innerObjClass).formatted(object.toString()));
        }
    }
}
