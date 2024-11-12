package com.github.sepgh.test.scheme;

import com.github.sepgh.testudo.scheme.ModelToSchemeCollectionConverter;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.annotation.AutoIncrement;
import com.github.sepgh.testudo.scheme.annotation.Collection;
import com.github.sepgh.testudo.scheme.annotation.Field;
import com.github.sepgh.testudo.scheme.annotation.Index;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ModelToSchemeCollectionConverterTestCase {

    @Getter
    @Collection(id = 1, name = "test")
    public static class TestModel {
        @Field(id = 1)
        @AutoIncrement
        @Index(primary = true)
        private int id;

        @Field(id = 2)
        private String name;

        @Field(id = 3)
        @Index()
        private long age;

        @Field(id = 4, name = "country_code")
        @Index(lowCardinality = true)
        private String country;
    }

    @Getter
    @Collection(id = 1, name = "test")
    public static class TestModel2 {
        @Field(id = 1)
        private TestModel test;
    }


    @Test
    public void convert() {
        ModelToSchemeCollectionConverter converter = new ModelToSchemeCollectionConverter(TestModel.class);
        Scheme.Collection collection = converter.toCollection();

        Assertions.assertEquals(1, collection.getId());
        Assertions.assertEquals("test", collection.getName());

        List<Scheme.Field> fields = collection.getFields();
        Assertions.assertEquals(4, fields.size());

        Scheme.Field field = fields.getFirst();
        Assertions.assertEquals(1, field.getId());
        Assertions.assertEquals("id", field.getName());
        Assertions.assertEquals("int", field.getType());
        Assertions.assertTrue(field.isIndex());
        Assertions.assertTrue(field.isPrimary());
        Assertions.assertTrue(field.isAutoIncrement());

        field = fields.get(1);
        Assertions.assertEquals(2, field.getId());
        Assertions.assertEquals("name", field.getName());
        Assertions.assertEquals("char", field.getType());

        field = fields.get(2);
        Assertions.assertEquals(3, field.getId());
        Assertions.assertEquals("long", field.getType());
        Assertions.assertEquals("age", field.getName());
        Assertions.assertTrue(field.isIndex());

        field = fields.get(3);
        Assertions.assertEquals(4, field.getId());
        Assertions.assertEquals("char", field.getType());
        Assertions.assertEquals("country_code", field.getName());
        Assertions.assertTrue(field.isIndex());
        Assertions.assertTrue(field.isLowCardinality());

        // Todo: runtime exception should not get thrown
        Assertions.assertThrows(RuntimeException.class, () -> {
            new ModelToSchemeCollectionConverter(TestModel2.class).toCollection();
        });
    }

}
