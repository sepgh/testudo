package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.index.Pointer;
import lombok.Getter;

@Getter
public abstract class NodeData<V extends Comparable<V>> {
    protected byte[] bytes;
    protected int beginning;

    public NodeData(byte[] bytes, int beginning) {
        this.bytes = new byte[this.size()];
        System.arraycopy(
                bytes,
                beginning,
                this.bytes,
                0,
                this.size()
        );
    }

    public NodeData(byte[] bytes) {
        this(bytes, 0);
    }

    public NodeData(V v){
        this.bytes = this.valueToByteArray(v);
        this.beginning = 0;
    }

    protected abstract byte[] valueToByteArray(V v);

    public abstract boolean exists();
    public abstract V data();
    public abstract int size();

    public static class InvalidValueForNodeInnerObj extends Exception {
        public InvalidValueForNodeInnerObj(Object object, Class<? extends NodeData<?>> innerObjClass) {
            super(("%s is not a valid value for NodeInnerObj of type " + innerObjClass).formatted(object.toString()));
        }
    }


    public interface Strategy<K extends Comparable<K>> {
        Class<? extends NodeData<K>> getNodeDataClass();
        Class<K> getDataClass();
        NodeData<K> fromObject(K k);
        NodeData<K> fromBytes(byte[] bytes, int beginning);
        default NodeData<K> fromBytes(byte[] bytes) {
            return this.fromBytes(bytes, 0);
        }
        int size();
        default boolean isValid(K identifier){return true;}

        NodeData.Strategy<Integer> INTEGER = new NodeData.Strategy<Integer>() {
            @Override
            public Class<? extends NodeData<Integer>> getNodeDataClass() {
                return IntegerIdentifier.class;
            }

            @Override
            public Class<Integer> getDataClass() {
                return Integer.TYPE;
            }

            @Override
            public NodeData<Integer> fromObject(Integer integer) {
                return new IntegerIdentifier(integer);
            }

            @Override
            public NodeData<Integer> fromBytes(byte[] bytes, int beginning) {
                return new IntegerIdentifier(bytes, beginning);
            }

            @Override
            public int size() {
                return IntegerIdentifier.BYTES;
            }
        };
        NodeData.Strategy<Long> LONG = new NodeData.Strategy<Long>() {
            @Override
            public Class<? extends NodeData<Long>> getNodeDataClass() {
                return LongIdentifier.class;
            }

            @Override
            public Class<Long> getDataClass() {
                return Long.TYPE;
            }

            @Override
            public NodeData<Long> fromObject(Long aLong) {
                return new LongIdentifier(aLong);
            }

            @Override
            public NodeData<Long> fromBytes(byte[] bytes, int beginning) {
                return new LongIdentifier(bytes, beginning);
            }
            @Override
            public int size() {
                return LongIdentifier.BYTES;
            }
        };

        NodeData.Strategy<Long> NO_ZERO_LONG = new NodeData.Strategy<Long>() {
            @Override
            public Class<? extends NodeData<Long>> getNodeDataClass() {
                return NoZeroLongIdentifier.class;
            }

            @Override
            public Class<Long> getDataClass() {
                return Long.TYPE;
            }

            @Override
            public NodeData<Long> fromObject(Long aLong) {
                return new NoZeroLongIdentifier(aLong);
            }

            @Override
            public int size() {
                return NoZeroLongIdentifier.BYTES;
            }

            @Override
            public NodeData<Long> fromBytes(byte[] bytes, int beginning) {
                return new NoZeroLongIdentifier(bytes, beginning);
            }
            @Override
            public boolean isValid(Long identifier) {
                return identifier != 0;
            }
        };

        NodeData.Strategy<Integer> NO_ZERO_INTEGER = new NodeData.Strategy<Integer>() {
            @Override
            public Class<? extends NodeData<Integer>> getNodeDataClass() {
                return NoZeroIntegerIdentifier.class;
            }

            @Override
            public Class<Integer> getDataClass() {
                return Integer.TYPE;
            }

            @Override
            public NodeData<Integer> fromObject(Integer integer) {
                return new NoZeroIntegerIdentifier(integer);
            }

            @Override
            public int size() {
                return NoZeroIntegerIdentifier.BYTES;
            }

            @Override
            public NodeData<Integer> fromBytes(byte[] bytes, int beginning) {
                return new NoZeroIntegerIdentifier(bytes, beginning);
            }
            @Override
            public boolean isValid(Integer identifier) {
                return identifier != 0;
            }
        };

        NodeData.Strategy<Pointer> POINTER = new NodeData.Strategy<Pointer>() {
            @Override
            public Class<? extends NodeData<Pointer>> getNodeDataClass() {
                return PointerInnerObject.class;
            }

            @Override
            public Class<Pointer> getDataClass() {
                return Pointer.class;
            }

            @Override
            public NodeData<Pointer> fromObject(Pointer pointer) {
                return new PointerInnerObject(pointer);
            }
            @Override
            public NodeData<Pointer> fromBytes(byte[] bytes, int beginning) {
                return new PointerInnerObject(bytes, beginning);
            }
            @Override
            public int size() {
                return PointerInnerObject.BYTES;
            }
        };

    }
}
