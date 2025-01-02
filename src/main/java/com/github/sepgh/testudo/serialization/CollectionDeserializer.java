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

        Bitmap<Integer> nullsBitmap = CollectionSerializationUtil.getNullsBitmap(collection, bytes);
        ArrayList<Integer> nullFieldIds = Lists.newArrayList(nullsBitmap.getOnIterator(Order.ASC));

        List<Scheme.Field> fields = collection.getFields();
        fields.sort(Comparator.comparingInt(Scheme.Field::getId));

        int fieldIndex = 0;
        for (Scheme.Field field : fields) {
            if (nullFieldIds.contains(fieldIndex)) {
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
