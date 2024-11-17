package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.scheme.ModelToCollectionConverter;
import com.github.sepgh.testudo.scheme.Scheme;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ModelDeserializer {
    private byte[] bytes;

    public ModelDeserializer(byte[] bytes) {
        this.reset(bytes);
    }

    public synchronized void reset(byte[] bytes) {
        this.bytes = bytes;
    }

    public <T> T deserialize(Class<T> tClass) throws SerializationException, DeserializationException {
        Scheme.Collection collection = new ModelToCollectionConverter(tClass).toCollection();

        T t;
        try {
            t = tClass.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new DeserializationException("Error while trying to get or invoke default constructor for class " + tClass.getName(), e);
        }

        for (Scheme.Field field : collection.getFields()) {
            Object fieldValue = CollectionSerializationUtil.getValueOfFieldAsObject(collection, field, bytes);

            Method method;
            try {
                method = tClass.getMethod("set" + field.getObjectFieldName().substring(0, 1).toUpperCase() + field.getObjectFieldName().substring(1), fieldValue.getClass());
            } catch (NoSuchMethodException e) {
                throw new DeserializationException("Could not find setter for field '%s' in class '%s'.".formatted(field.getObjectFieldName(), tClass.getName()), e);
            }

            try {
                method.invoke(t, fieldValue);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new DeserializationException("Failed to invoke setter for field '%s' in class '%s'.".formatted(field.getObjectFieldName(), tClass.getName()), e);
            }

        }

        return t;
    }

}
