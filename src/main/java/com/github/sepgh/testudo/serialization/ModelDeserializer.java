package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.scheme.ModelToCollectionConverter;
import com.github.sepgh.testudo.scheme.Scheme;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class ModelDeserializer<T> {
    private final Class<T> tClass;

    public ModelDeserializer(Class<T> tClass) {
        this.tClass = tClass;
    }

    public T deserialize(byte[] bytes) throws SerializationException, DeserializationException {
        Scheme.Collection collection = new ModelToCollectionConverter(tClass).toCollection();
        CollectionDeserializer deserializer = new CollectionDeserializer(collection);
        Map<String, Object> fieldValueMap = deserializer.deserialize(bytes);
        T t;
        try {
            t = tClass.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new DeserializationException("Error while trying to get or invoke default constructor for class " + tClass.getName(), e);
        }

        for (Scheme.Field field : collection.getFields()) {
            Method method;
            Object fieldValue = fieldValueMap.get(field.getName());

            if (fieldValue == null) {
                continue;
            }

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
