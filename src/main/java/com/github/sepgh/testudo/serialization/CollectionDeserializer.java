package com.github.sepgh.testudo.serialization;

import com.github.sepgh.testudo.ds.Bitmap;
import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.scheme.Scheme;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;

import java.util.*;


@AllArgsConstructor
public class CollectionDeserializer {
    private final Scheme.Collection collection;

    public Map<String, Object> deserialize(byte[] bytes) throws DeserializationException {
        Map<String, Object> result = new HashMap<>();

        int nullsLen = bytes.length - CollectionSerializationUtil.getSizeOfCollection(collection);
        byte[] nulls = new byte[nullsLen];
        System.arraycopy(bytes, bytes.length - nullsLen, nulls, 0, nullsLen);
        Bitmap<Integer> nullsBitmap = new Bitmap<>(Integer.class, nulls);
        ArrayList<Integer> nullFieldIds = Lists.newArrayList(nullsBitmap.getOnIterator(Order.ASC));

        List<Scheme.Field> fields = collection.getFields();
        fields.sort(Comparator.comparingInt(Scheme.Field::getId));

        int fieldIndex = 0;
        for (Scheme.Field field : fields) {
            if (nullFieldIds.contains(fieldIndex)) {
                System.out.println("[DeSerializer] Null field: " + field.getName());
                result.put(field.getName(), null);
            } else {
                result.put(
                        field.getName(),
                        CollectionSerializationUtil.getValueOfFieldAsObject(collection, field, bytes)
                );
            }

            fieldIndex++;
        }
        return result;
    }

}
