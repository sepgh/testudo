package com.github.sepgh.internal.index.tree.node.data;

import com.github.sepgh.internal.index.Pointer;
import lombok.Getter;

@Getter
public abstract class NodeInnerObj<V extends Comparable<V>> {
    protected byte[] bytes;
    protected int beginning;

    public NodeInnerObj(byte[] bytes, int beginning) {
        this.bytes = new byte[this.size()];
        System.arraycopy(
                bytes,
                beginning,
                this.bytes,
                0,
                this.size()
        );
    }

    public NodeInnerObj(byte[] bytes) {
        this(bytes, 0);
    }

    public NodeInnerObj(V v){
        this.bytes = this.valueToByteArray(v);
        this.beginning = 0;
    }

    protected abstract byte[] valueToByteArray(V v);

    public abstract boolean exists();
    public abstract V data();
    public abstract int size();


    public interface Strategy<E extends Comparable<E>> {
        Class<? extends NodeInnerObj<E>> getNodeInnerObjClass();
        Class<E> getValueClass();
        NodeInnerObj<E> fromObject(E e);
        int size();

        NodeInnerObj.Strategy<Integer> INTEGER = new NodeInnerObj.Strategy<Integer>() {
            @Override
            public Class<? extends NodeInnerObj<Integer>> getNodeInnerObjClass() {
                return IntegerIdentifier.class;
            }

            @Override
            public Class<Integer> getValueClass() {
                return Integer.TYPE;
            }

            @Override
            public NodeInnerObj<Integer> fromObject(Integer integer) {
                return new IntegerIdentifier(integer);
            }

            @Override
            public int size() {
                return IntegerIdentifier.BYTES;
            }
        };
        NodeInnerObj.Strategy<Long> LONG = new NodeInnerObj.Strategy<Long>() {
            @Override
            public Class<? extends NodeInnerObj<Long>> getNodeInnerObjClass() {
                return LongIdentifier.class;
            }

            @Override
            public Class<Long> getValueClass() {
                return Long.TYPE;
            }

            @Override
            public NodeInnerObj<Long> fromObject(Long aLong) {
                return new LongIdentifier(aLong);
            }

            @Override
            public int size() {
                return LongIdentifier.BYTES;
            }
        };

        NodeInnerObj.Strategy<Long> NO_ZERO_LONG = new NodeInnerObj.Strategy<Long>() {
            @Override
            public Class<? extends NodeInnerObj<Long>> getNodeInnerObjClass() {
                return NoZeroLongIdentifier.class;
            }

            @Override
            public Class<Long> getValueClass() {
                return Long.TYPE;
            }

            @Override
            public NodeInnerObj<Long> fromObject(Long aLong) {
                return new NoZeroLongIdentifier(aLong);
            }

            @Override
            public int size() {
                return NoZeroLongIdentifier.BYTES;
            }
        };

        NodeInnerObj.Strategy<Integer> NO_ZERO_INTEGER = new NodeInnerObj.Strategy<Integer>() {
            @Override
            public Class<? extends NodeInnerObj<Integer>> getNodeInnerObjClass() {
                return NoZeroIntegerIdentifier.class;
            }

            @Override
            public Class<Integer> getValueClass() {
                return Integer.TYPE;
            }

            @Override
            public NodeInnerObj<Integer> fromObject(Integer integer) {
                return new NoZeroIntegerIdentifier(integer);
            }

            @Override
            public int size() {
                return NoZeroIntegerIdentifier.BYTES;
            }
        };

        NodeInnerObj.Strategy<Pointer> POINTER = new NodeInnerObj.Strategy<Pointer>() {
            @Override
            public Class<? extends NodeInnerObj<Pointer>> getNodeInnerObjClass() {
                return PointerInnerObject.class;
            }

            @Override
            public Class<Pointer> getValueClass() {
                return Pointer.class;
            }

            @Override
            public NodeInnerObj<Pointer> fromObject(Pointer pointer) {
                return new PointerInnerObject(pointer);
            }

            @Override
            public int size() {
                return PointerInnerObject.BYTES;
            }
        };
    }
}
