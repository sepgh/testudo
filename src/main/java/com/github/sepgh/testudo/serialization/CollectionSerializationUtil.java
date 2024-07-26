package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.storage.db.DBObject;

import javax.annotation.Nullable;
import java.util.List;


public class CollectionSerializationUtil {


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

    public static int getSizeOfCollection(Scheme.Collection collection) {
        int size = 0;
        for (Scheme.Field field : collection.getFields()) {
            size += getByteArrSizeOfField(field);
        }
        return size;
    }

    public static void removeField(Scheme.Collection collection, Scheme.Field fieldToRemove, DBObject dbObject) {
        List<Scheme.Field> fields = collection.getFields();
        int i = fields.indexOf(fieldToRemove);

        int offset = getByteArrOffsetTillFieldIndex(fields, i);
        int size = getByteArrSizeOfField(fieldToRemove);
        dbObject.modifyData(offset, new byte[size]);

        if (i != fields.size() - 1) {
            int newOffset = offset + size;
            byte[] bytes = dbObject.readData(newOffset, dbObject.getDataSize() - newOffset);
            dbObject.modifyData(newOffset, bytes);
        }
    }

    public static boolean hasSpaceToChangeType(Scheme.Collection collection, Scheme.Field oldField, Scheme.Field newField, DBObject dbObject) {
        Serializer<?> oldTypeSerializer = SerializerRegistry.getInstance().getSerializer(oldField.getType());
        Serializer<?> newTypeSerializer = SerializerRegistry.getInstance().getSerializer(newField.getType());

        int requiredAdditionalSpace = newTypeSerializer.getSize(newField.getMeta()) - oldTypeSerializer.getSize(oldField.getMeta());

        if (requiredAdditionalSpace > 0) {
            // Check space
            return getSizeOfCollection(collection) <= dbObject.getDataSize() + requiredAdditionalSpace;
        }

        return true;
    }

    public static void setValueOfField(Scheme.Collection after, Scheme.Field field, byte[] obj, @Nullable byte[] bytes) throws SerializationException {
        int offset = getByteArrOffsetTillFieldIndex(after.getFields(), after.getFields().indexOf(field));
        int size = getByteArrSizeOfField(field);
        if (bytes != null) {
            if (bytes.length > size) {
                System.arraycopy(bytes, 0, bytes, 0, size);
            }
        } else {
            Serializer<?> serializer = SerializerRegistry.getInstance().getSerializer(field.getType());
            assert serializer != null;
            bytes = serializer.serializeDefault(field.getDefaultValue(), field.getMeta());
        }

        System.arraycopy(bytes, 0, obj, offset, bytes.length);
    }
}
