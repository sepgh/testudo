package com.github.sepgh.test.serializer;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.annotation.Collection;
import com.github.sepgh.testudo.scheme.annotation.Field;
import com.github.sepgh.testudo.scheme.annotation.Index;
import com.github.sepgh.testudo.serialization.CharArrSerializer;
import com.github.sepgh.testudo.serialization.ModelDeserializer;
import com.github.sepgh.testudo.serialization.ModelSerializer;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import lombok.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class ModelSerializerTestCase {

    @Data
    @Collection(id = 1, name = "test")
    @Builder
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestModel {
        @Field(id = 1)
        @Index(primary = true, autoIncrement = true)
        private Integer id;

        @Field(id = 2, maxLength = 20)
        private String name;

        @Field(id = 3)
        @Index()
        private Long age;

        @Field(id = 4, name = "country_code")
        @Index(lowCardinality = true)
        private String country;

    }


    @Test
    public void serialize() throws SerializationException, DeserializationException {
        ModelSerializerTestCase.TestModel model = TestModel.builder()
                .id(11)
                .name("test")
                .age(12L)
                .country("country")
                .build();

        ModelSerializer modelSerializer = new ModelSerializer(model);
        byte[] serialized = modelSerializer.serialize();

        byte[] id = new byte[4];
        System.arraycopy(serialized, 0, id, 0, 4);
        Assertions.assertEquals(model.getId(), Ints.fromByteArray(id));

        byte[] name = new byte[20];
        System.arraycopy(serialized, 4, name, 0, 20);
        String deserializedName = new CharArrSerializer().deserialize(name, Scheme.Meta.builder().maxLength(20).charset(StandardCharsets.UTF_8.name()).build());
        Assertions.assertEquals(model.getName(), deserializedName);

        byte[] age = new byte[8];
        System.arraycopy(serialized, 24, age, 0, 8);
        Assertions.assertEquals(model.getAge(), Longs.fromByteArray(age));

        byte[] countryCode = new byte[CharArrSerializer.MAX_LENGTH];
        System.arraycopy(serialized, 24 + 8, countryCode, 0, CharArrSerializer.MAX_LENGTH);
        String deserializedCountryCode = new CharArrSerializer().deserialize(countryCode, Scheme.Meta.builder().maxLength(20).charset(StandardCharsets.UTF_8.name()).build());
        Assertions.assertEquals(model.getCountry(), deserializedCountryCode);

    }

    // Depends on serialize to work properly
    @Test
    public void deserialize() throws SerializationException, DeserializationException {
        ModelSerializerTestCase.TestModel model = TestModel.builder()
                .id(11)
                .name("test")
                .age(12L)
                .country("country")
                .build();

        ModelSerializer modelSerializer = new ModelSerializer(model);
        byte[] serialized = modelSerializer.serialize();

        ModelDeserializer<TestModel> modelDeserializer = new ModelDeserializer<>(TestModel.class);
        TestModel deserialize = modelDeserializer.deserialize(serialized);

        Assertions.assertEquals(model.getId(), deserialize.getId());
        Assertions.assertEquals(model.getName(), deserialize.getName());
        Assertions.assertEquals(model.getAge(), deserialize.getAge());
        Assertions.assertEquals(model.getCountry(), deserialize.getCountry());
        Assertions.assertEquals(model, deserialize);
    }

}
