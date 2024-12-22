package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.ds.Bitmap;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.scheme.ModelToCollectionConverter;
import com.github.sepgh.testudo.scheme.Scheme;

import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class ModelSerializer {

    private Object model;
    private ModelToCollectionConverter modelToCollection;

    public ModelSerializer() {
    }

    public ModelSerializer(Object model) {
        this.reset(model);
    }

    public synchronized ModelSerializer reset(Object model) {
        this.model = model;
        if (this.modelToCollection == null || !this.modelToCollection.getModelClass().equals(model.getClass()))
            this.modelToCollection = new ModelToCollectionConverter(model.getClass());
        return this;
    }

    public byte[] serialize() throws SerializationException {
        Scheme.Collection collection = modelToCollection.toCollection();
        List<Scheme.Field> fields = collection.getFields();
        fields.sort(Comparator.comparingInt(Scheme.Field::getId));

        int nullsBitmapSize = (int) Math.ceil((double) fields.size() / 8);
        byte[] bytes = new byte[CollectionSerializationUtil.getSizeOfCollection(collection)];
        byte[] nulls = new byte[nullsBitmapSize];
        Bitmap<Integer> nullsBitmap = new Bitmap<>(Integer.class, nulls);

        int fieldIndex = 0;
        for (Scheme.Field field : fields) {
            Optional<byte[]> optionalFieldValue = getFieldValue(field);

            if (optionalFieldValue.isPresent()) {
                CollectionSerializationUtil.setValueOfField(collection, field, bytes, optionalFieldValue.get());
            } else {
                if (field.isNullable()) {
                    nullsBitmap.on(fieldIndex);
                } else {
                    throw new SerializationException("Field " + field.getName() + " is not nullable");
                }
            }

            fieldIndex++;
        }

        return this.mergeDataAndNulls(bytes, nullsBitmap.getData());
    }

    private byte[] mergeDataAndNulls(byte[] bytes, byte[] nulls) {
        byte[] newBytes = new byte[bytes.length + nulls.length];
        System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
        System.arraycopy(nulls, 0, newBytes, bytes.length, nulls.length);
        return newBytes;
    }

    @SuppressWarnings("unchecked")
    private <T extends Comparable<T>> Optional<byte[]> getFieldValue(Scheme.Field field) throws SerializationException {
        Object invoked = null;
        try {
            invoked = this.model.getClass().getMethod(
                    "get" + field.getObjectFieldName().substring(0, 1).toUpperCase() + field.getObjectFieldName().substring(1)
            ).invoke(model);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new SerializationException(e);
        }

        if (invoked == null)
            return Optional.empty();

        Serializer<T> serializer = (Serializer<T>) SerializerRegistry.getInstance().getSerializer(field.getType());
        return Optional.of(
                serializer.serialize((T) invoked, field.getMeta())
        );
    }

}
