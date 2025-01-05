package com.github.sepgh.testudo.ds;

import com.github.sepgh.testudo.operation.query.Order;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;
import lombok.Getter;

import java.math.BigInteger;
import java.util.ListIterator;
import java.util.NoSuchElementException;

@Getter
public class Bitmap<K extends Number> {
    private final Class<K> kClass;
    private byte[] data;
    private int width;

    public Bitmap(Class<K> kClass, byte[] data) {
        this.kClass = kClass;
        this.data = data;
        this.width = data.length * Byte.SIZE;
    }

    @SuppressWarnings("unchecked")
    public static <K extends Number> Bitmap<K> getGenericInstance(K on) {
        Bitmap<K> output = null;
        if (on instanceof Integer) {
            output = (Bitmap<K>) new Bitmap<>(Integer.class, new byte[10]);
        }
        if (on instanceof Long) {
            output = (Bitmap<K>) new Bitmap<>(Long.class, new byte[10]);
        }
        if (on instanceof UnsignedInteger) {
            output = (Bitmap<K>) new Bitmap<>(UnsignedInteger.class, new byte[10]);
        }
        if (on instanceof UnsignedLong) {
            output = (Bitmap<K>) new Bitmap<>(UnsignedLong.class, new byte[10]);
        }
        if (output != null){
            output.on(on);
            return output;
        }
        throw new UnsupportedOperationException(); // Todo
    }

    public boolean on(K k) {
        BigInteger bitIndex = convertKeyToBigInteger(k);  // Convert to BigInteger for safety
        ensureCapacity(bitIndex);  // Resize if necessary

        // Calculate the byte and bit position
        int byteIndex = bitIndex.divide(BigInteger.valueOf(Byte.SIZE)).intValue();
        int bitPosition = bitIndex.mod(BigInteger.valueOf(Byte.SIZE)).intValue();

        if ((data[byteIndex] & (1 << bitPosition)) != 0)
            return false;

        // Set the bit
        data[byteIndex] |= (byte) (1 << bitPosition);
        return true;
    }

    public boolean isOn(K k) {
        BigInteger bitIndex = convertKeyToBigInteger(k);  // Convert to BigInteger for safety
        ensureCapacity(bitIndex);  // Resize if necessary

        // Calculate the byte and bit position
        int byteIndex = bitIndex.divide(BigInteger.valueOf(Byte.SIZE)).intValue();
        int bitPosition = bitIndex.mod(BigInteger.valueOf(Byte.SIZE)).intValue();

        return ((data[byteIndex] & (1 << bitPosition)) != 0);
    }

    public boolean off(K k) {
        BigInteger bitIndex = convertKeyToBigInteger(k);  // Convert to BigInteger for safety

        if (needsExpansion(bitIndex)){
            return false;
        }

        // Calculate the byte and bit position
        int byteIndex = bitIndex.divide(BigInteger.valueOf(Byte.SIZE)).intValue();
        int bitPosition = bitIndex.mod(BigInteger.valueOf(Byte.SIZE)).intValue();

        if ((data[byteIndex] & (1 << bitPosition)) == 0){
            return false;
        }

        // Clear the bit
        data[byteIndex] &= (byte) ~(1 << bitPosition);
        return true;
    }

    // Convert K to a BigInteger, handling UnsignedLong values beyond Long.MAX_VALUE
    private BigInteger convertKeyToBigInteger(K k) {
        if (k instanceof UnsignedLong) {
            return new BigInteger(((UnsignedLong) k).toString());  // Handle UnsignedLong safely
        }
        return BigInteger.valueOf(k.longValue());  // Handles Long, Integer, etc.
    }

    private boolean needsExpansion(BigInteger bitIndex) {
        return bitIndex.compareTo(BigInteger.valueOf(width)) >= 0;
    }

    // Ensure the byte array has enough space to handle the bit at bitIndex
    private void ensureCapacity(BigInteger bitIndex) {
        if (needsExpansion(bitIndex)) {
            // We need to grow the array to accommodate the new bit index
            int newWidth = bitIndex.intValue() + 1;  // At least one more than the current bit index
            int newLength = (newWidth + Byte.SIZE - 1) / Byte.SIZE;  // Convert bits to bytes

            // Grow the array and update width
            byte[] newData = new byte[newLength];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
            width = newLength * Byte.SIZE;
        }
    }

    // Iterator to return indices of all 'on' bits (1s)
    public ListIterator<K> getOnIterator(Order order) {
        return switch (order) {
            case DESC -> new DescendingBitIterator(true);
            case ASC -> new AscendingBitIterator(true);
        };
    }

    // Iterator to return indices of all 'off' bits (0s)
    public ListIterator<K> getOffIterator(Order order) {
        return switch (order) {
            case DESC -> new DescendingBitIterator(false);
            case ASC -> new AscendingBitIterator(false);
        };
    }

    // Iterator class that separates bit and byte index handling
    private class AscendingBitIterator implements ListIterator<K> {
        private int currentByteIndex = 0;
        private int currentBitPosition = 0;
        protected final boolean checkForOnBit;  // true for 'on' iterator, false for 'off' iterator
        protected BigInteger lastReturnedBitIndex = BigInteger.valueOf(-1);

        public AscendingBitIterator(boolean checkForOnBit) {
            this.checkForOnBit = checkForOnBit;
        }

        @Override
        public boolean hasNext() {
            int tempByteIndex = currentByteIndex;
            int tempBitPosition = currentBitPosition;

            while (tempByteIndex < data.length) {
                boolean isBitSet = (data[tempByteIndex] & (1 << tempBitPosition)) != 0;
                if ((isBitSet && checkForOnBit) || (!isBitSet && !checkForOnBit)) {
                    return true;
                }
                // Move to the next bit
                tempBitPosition++;
                if (tempBitPosition >= Byte.SIZE) {
                    tempBitPosition = 0;
                    tempByteIndex++;
                }
            }
            return false;
        }

        @Override
        public K next() {
            while (currentByteIndex < data.length) {
                boolean isBitSet = (data[currentByteIndex] & (1 << currentBitPosition)) != 0;

                if ((isBitSet && checkForOnBit) || (!isBitSet && !checkForOnBit)) {
                    lastReturnedBitIndex = BigInteger.valueOf(currentByteIndex).multiply(BigInteger.valueOf(Byte.SIZE)).add(BigInteger.valueOf(currentBitPosition));
                    moveToNextBit();
                    return convertIndexToK(lastReturnedBitIndex);
                }

                moveToNextBit();
            }
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasPrevious() {
            int tempByteIndex = currentByteIndex;
            int tempBitPosition = currentBitPosition;

            // Move one step back to simulate previous position
            if (tempBitPosition == 0) {
                tempByteIndex--;
                tempBitPosition = Byte.SIZE - 1;
            } else {
                tempBitPosition--;
            }

            while (tempByteIndex >= 0) {
                boolean isBitSet = (data[tempByteIndex] & (1 << tempBitPosition)) != 0;
                if ((isBitSet && checkForOnBit) || (!isBitSet && !checkForOnBit)) {
                    return true;
                }
                // Move to the previous bit
                if (tempBitPosition == 0) {
                    tempByteIndex--;
                    tempBitPosition = Byte.SIZE - 1;
                } else {
                    tempBitPosition--;
                }
            }
            return false;
        }

        @Override
        public K previous() {
            if (!hasPrevious()) {
                throw new NoSuchElementException();
            }

            // Move one step back
            if (currentBitPosition == 0) {
                currentByteIndex--;
                currentBitPosition = Byte.SIZE - 1;
            } else {
                currentBitPosition--;
            }

            while (currentByteIndex >= 0) {
                boolean isBitSet = (data[currentByteIndex] & (1 << currentBitPosition)) != 0;

                if ((isBitSet && checkForOnBit) || (!isBitSet && !checkForOnBit)) {
                    lastReturnedBitIndex = BigInteger.valueOf(currentByteIndex).multiply(BigInteger.valueOf(Byte.SIZE)).add(BigInteger.valueOf(currentBitPosition));
                    return convertIndexToK(lastReturnedBitIndex);
                }

                if (currentBitPosition == 0) {
                    currentByteIndex--;
                    currentBitPosition = Byte.SIZE - 1;
                } else {
                    currentBitPosition--;
                }
            }
            throw new NoSuchElementException();
        }

        @Override
        public int nextIndex() {
            return lastReturnedBitIndex.add(BigInteger.valueOf(1)).intValue();
        }

        @Override
        public int previousIndex() {
            return lastReturnedBitIndex.subtract(BigInteger.valueOf(1)).intValue();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() is not implemented.");
        }

        @Override
        public void set(K k) {
            throw new UnsupportedOperationException("set() is not implemented.");
        }

        @Override
        public void add(K k) {
            throw new UnsupportedOperationException("add() is not implemented.");
        }

        private void moveToNextBit() {
            currentBitPosition++;
            if (currentBitPosition >= Byte.SIZE) {
                currentBitPosition = 0;
                currentByteIndex++;
            }
        }

        // Convert BigInteger index to K type
        @SuppressWarnings("unchecked")
        protected K convertIndexToK(BigInteger index) {
            if (UnsignedLong.class.isAssignableFrom(kClass)) {
                return (K) UnsignedLong.valueOf(index);  // Handle UnsignedLong
            } else if (UnsignedInteger.class.isAssignableFrom(kClass)) {
                return (K) UnsignedInteger.valueOf(index);  // Handle UnsignedInteger
            } else if (Long.class.isAssignableFrom(kClass)) {
                return (K) Long.valueOf(index.longValue());  // Handle Long
            }
            return (K) Integer.valueOf(index.intValue());  // Default handling for Long/Integer
        }
    }

    private class DescendingBitIterator extends AscendingBitIterator {
        private int currentByteIndex;
        private int currentBitPosition;
        protected BigInteger lastReturnedBitIndex = BigInteger.valueOf(-1);

        public DescendingBitIterator(boolean checkForOnBit) {
            super(checkForOnBit);
            this.currentByteIndex = data.length - 1;  // Start from the highest byte index
            this.currentBitPosition = Byte.SIZE - 1;  // Start from the highest bit position in the byte
        }

        @Override
        public boolean hasNext() {
            int tempByteIndex = currentByteIndex;
            int tempBitPosition = currentBitPosition;

            while (tempByteIndex >= 0) {
                boolean isBitSet = (data[tempByteIndex] & (1 << tempBitPosition)) != 0;
                if ((isBitSet && checkForOnBit) || (!isBitSet && !checkForOnBit)) {
                    return true;
                }

                // Move to the previous bit
                tempBitPosition--;
                if (tempBitPosition < 0) {
                    tempBitPosition = Byte.SIZE - 1;
                    tempByteIndex--;
                }
            }
            return false;
        }

        @Override
        public K next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            while (currentByteIndex >= 0) {
                boolean isBitSet = (data[currentByteIndex] & (1 << currentBitPosition)) != 0;

                if ((isBitSet && checkForOnBit) || (!isBitSet && !checkForOnBit)) {
                    lastReturnedBitIndex = BigInteger.valueOf(currentByteIndex).multiply(BigInteger.valueOf(Byte.SIZE)).add(BigInteger.valueOf(currentBitPosition));                    moveToPreviousBit();
                    return convertIndexToK(lastReturnedBitIndex);
                }

                moveToPreviousBit();
            }
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasPrevious() {
            int tempByteIndex = currentByteIndex;
            int tempBitPosition = currentBitPosition;

            // Move one step forward to simulate the next position
            tempBitPosition++;
            if (tempBitPosition >= Byte.SIZE) {
                tempBitPosition = 0;
                tempByteIndex++;
            }

            while (tempByteIndex < data.length) {
                boolean isBitSet = (data[tempByteIndex] & (1 << tempBitPosition)) != 0;
                if ((isBitSet && checkForOnBit) || (!isBitSet && !checkForOnBit)) {
                    return true;
                }

                // Move to the next bit
                tempBitPosition++;
                if (tempBitPosition >= Byte.SIZE) {
                    tempBitPosition = 0;
                    tempByteIndex++;
                }
            }
            return false;
        }

        @Override
        public K previous() {
            if (!hasPrevious()) {
                throw new NoSuchElementException();
            }

            // Move one step forward
            currentBitPosition++;
            if (currentBitPosition >= Byte.SIZE) {
                currentBitPosition = 0;
                currentByteIndex++;
            }

            while (currentByteIndex < data.length) {
                boolean isBitSet = (data[currentByteIndex] & (1 << currentBitPosition)) != 0;

                if ((isBitSet && checkForOnBit) || (!isBitSet && !checkForOnBit)) {
                    lastReturnedBitIndex = BigInteger.valueOf(currentByteIndex).multiply(BigInteger.valueOf(Byte.SIZE)).add(BigInteger.valueOf(currentBitPosition));
                    return convertIndexToK(lastReturnedBitIndex);
                }

                if (currentBitPosition == Byte.SIZE - 1) {
                    currentByteIndex++;
                    currentBitPosition = 0;
                } else {
                    currentBitPosition++;
                }
            }
            throw new NoSuchElementException();
        }

        @Override
        public int nextIndex() {
            return lastReturnedBitIndex.subtract(BigInteger.ONE).intValue();
        }

        @Override
        public int previousIndex() {
            return lastReturnedBitIndex.add(BigInteger.ONE).intValue();
        }

        private void moveToPreviousBit() {
            currentBitPosition--;
            if (currentBitPosition < 0) {
                currentBitPosition = Byte.SIZE - 1;
                currentByteIndex--;
            }
        }

    }

}
