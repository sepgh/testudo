package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.ds.Bitmap;
import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.storage.db.DBObject;

import javax.annotation.Nullable;
import java.util.List;


public class CollectionSerializationUtil {

    public static Bitmap<Integer> getNullsBitmap(Scheme.Collection collection, byte[] bytes) {
        int nullsLen = bytes.length - CollectionSerializationUtil.getSizeOfCollection(collection);
        byte[] nulls = new byte[nullsLen];
        System.arraycopy(bytes, bytes.length - nullsLen, nulls, 0, nullsLen);
        return new Bitmap<>(Integer.class, nulls);
    }

    public static void setNullsBitmap(Scheme.Collection collection, byte[] bytes, byte[] bitmapBytes) {
        int nullsIndex = CollectionSerializationUtil.getSizeOfCollection(collection) - 1;
        System.arraycopy(bitmapBytes, 0, bytes, nullsIndex, bitmapBytes.length);
    }

    public static boolean areTypesCompatible(String t1, String t2) {
        Serializer<?> serializer = SerializerRegistry.getInstance().getSerializer(t1);
        assert serializer != null;
        return serializer.compatibleTypes().contains(t2);
    }

    public static int getByteArrSizeOfField(Scheme.Field field){
        Serializer<?> serializer = SerializerRegistry.getInstance().getSerializer(field.getType());
        assert serializer != null;
        return serializer.getSize(field.getMeta());
    }

    public static int getByteArrOffsetTillFieldIndex(List<Scheme.Field> fields, int index) {
        // Todo: if field would not be in collection and this method is called (probably with index: -1) then offset would be 0
        //       That should either be fixed here or at caller side

        if (index == 0)
            return 0;

        int offset = 0;

        for (int i = 0; i < index; i++){
            offset += getByteArrSizeOfField(fields.get(i));
        }

        return offset;
    }

    public static byte[] getValueOfField(Scheme.Collection collection, Scheme.Field field, DBObject dbObject){
        int offset = getByteArrOffsetTillFieldIndex(collection.getFields(), collection.getFields().indexOf(field));
        int size = getByteArrSizeOfField(field);
        return dbObject.readData(offset, size);
    }

    public static byte[] getValueOfField(Scheme.Collection collection, Scheme.Field field, byte[] obj){
        int offset = getByteArrOffsetTillFieldIndex(collection.getFields(), collection.getFields().indexOf(field));
        int size = getByteArrSizeOfField(field);
        byte[] output = new byte[size];
        System.arraycopy(obj, offset, output, 0, size);
        return output;
    }

    public static <V extends Comparable<V>> V getValueOfFieldAsObject(Scheme.Collection collection, Scheme.Field field, byte[] bytes) throws DeserializationException {
        Serializer<V> serializer = (Serializer<V>) SerializerRegistry.getInstance().getSerializer(field.getType());
        byte[] output = getValueOfField(collection, field, bytes);
        return serializer.deserialize(output, field.getMeta());
    }

    public static int getSizeOfCollection(Scheme.Collection collection) {
        int size = 0;
        for (Scheme.Field field : collection.getFields()) {
            size += getByteArrSizeOfField(field);
        }
        return size;
    }

    public static void setValueOfField(Scheme.Collection collection, Scheme.Field field, byte[] obj, @Nullable byte[] value) throws SerializationException {
        int offset = getByteArrOffsetTillFieldIndex(collection.getFields(), collection.getFields().indexOf(field));
        int size = getByteArrSizeOfField(field);
        if (value != null) {
            if (value.length > size) {
                System.arraycopy(value, 0, value, 0, size);
            }
        } else {
            Serializer<?> serializer = SerializerRegistry.getInstance().getSerializer(field.getType());
            assert serializer != null;
            value = serializer.serializeDefault(field.getDefaultValue(), field.getMeta());
        }

        System.arraycopy(value, 0, obj, offset, value.length);
    }
}
