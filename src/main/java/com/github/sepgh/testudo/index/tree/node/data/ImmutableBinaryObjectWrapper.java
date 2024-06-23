package com.github.sepgh.testudo.index.tree.node.data;

public interface ImmutableBinaryObjectWrapper<E extends Comparable<E>> {
    ImmutableBinaryObjectWrapper<E> load(E e) throws InvalidBinaryObjectWrapperValue;
    ImmutableBinaryObjectWrapper<E> load(byte[] bytes, int beginning);
    default ImmutableBinaryObjectWrapper<E> load(byte[] bytes) {
        return this.load(bytes, 0);
    }
    E asObject();
    boolean hasValue();
    int size();
    byte[] getBytes();

    class InvalidBinaryObjectWrapperValue extends Exception {
        public InvalidBinaryObjectWrapperValue(Object object, Class<? extends ImmutableBinaryObjectWrapper<?>> innerObjClass) {
            super(("%s is not a valid value for BinaryObjectWrapper of type " + innerObjClass).formatted(object.toString()));
        }
    }
}
