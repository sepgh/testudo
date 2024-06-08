package com.github.sepgh.internal.index.tree.node.cluster;

import com.github.sepgh.internal.index.tree.node.data.*;

public interface ClusterIdentifier {
    Strategy<Integer> INTEGER = new Strategy<Integer>() {
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
    Strategy<Long> LONG = new Strategy<Long>() {
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

    Strategy<Long> NO_ZERO_LONG = new Strategy<Long>() {
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

    Strategy<Integer> NO_ZERO_INTEGER = new Strategy<Integer>() {
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

    interface Strategy<E extends Comparable<E>> {
        Class<? extends NodeInnerObj<E>> getNodeInnerObjClass();
        Class<E> getValueClass();
        NodeInnerObj<E> fromObject(E e);
        int size();
    }

}
