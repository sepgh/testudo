package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.scheme.ModelToCollectionConverter;
import com.github.sepgh.testudo.scheme.Scheme;

import java.lang.reflect.InvocationTargetException;

public class ModelSerializer {

    private Object model;
    private ModelToCollectionConverter modelToCollection;

    public ModelSerializer(Object model) {
        this.reset(model);
    }

    public synchronized void reset(Object model) {
        this.model = model;
        modelToCollection = new ModelToCollectionConverter(model.getClass());
    }

    public byte[] serialize() throws SerializationException {
        Scheme.Collection collection = modelToCollection.toCollection();
        byte[] bytes = new byte[CollectionSerializationUtil.getSizeOfCollection(collection)];

        for (Scheme.Field field : collection.getFields()) {
            CollectionSerializationUtil.setValueOfField(collection, field, bytes, getFieldValue(field));
        }

        return bytes;
    }

    @SuppressWarnings("unchecked")
    private <T extends Comparable<T>> byte[] getFieldValue(Scheme.Field field) throws SerializationException {
        Object invoked = null;
        try {
            invoked = this.model.getClass().getMethod(
                    "get" + field.getObjectFieldName().substring(0, 1).toUpperCase() + field.getObjectFieldName().substring(1)
            ).invoke(model);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new SerializationException(e);
        }

        Serializer<T> serializer = (Serializer<T>) SerializerRegistry.getInstance().getSerializer(field.getType());
        return serializer.serialize((T) invoked, field.getMeta());
    }

}
