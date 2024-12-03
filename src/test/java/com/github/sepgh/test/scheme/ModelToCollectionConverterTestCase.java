package com.github.sepgh.test.scheme;

import com.github.sepgh.testudo.scheme.ModelToCollectionConverter;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.annotation.Collection;
import com.github.sepgh.testudo.scheme.annotation.Field;
import com.github.sepgh.testudo.scheme.annotation.Index;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ModelToCollectionConverterTestCase {

    @Getter
    @Collection(id = 1, name = "test")
    public static class TestModel implements Comparable<TestModel> {
        @Field(id = 1)
        @Index(primary = true, autoIncrement = true)
        private Integer id;

        @Field(id = 2)
        private String name;

        @Field(id = 3)
        @Index()
        private Long age;

        @Field(id = 4, name = "country_code")
        @Index(lowCardinality = true)
        private String country;

        @Override
        public int compareTo(TestModel testModel) {
            return 0;
        }
    }

    @Getter
    @Collection(id = 1, name = "test")
    public static class TestModel2 {
        @Field(id = 1)
        private TestModel test;
    }


    @Test
    public void convert() {
        ModelToCollectionConverter converter = new ModelToCollectionConverter(TestModel.class);
        Scheme.Collection collection = converter.toCollection();

        Assertions.assertEquals(1, collection.getId());
        Assertions.assertEquals("test", collection.getName());

        List<Scheme.Field> fields = collection.getFields();
        Assertions.assertEquals(4, fields.size());

        Scheme.Field field = fields.getFirst();
        Assertions.assertEquals(1, field.getId());
        Assertions.assertEquals("id", field.getName());
        Assertions.assertEquals("int", field.getType());
        Assertions.assertTrue(field.isIndexed());
        Assertions.assertTrue(field.getIndex().isPrimary());
        Assertions.assertTrue(field.getIndex().isAutoIncrement());

        field = fields.get(1);
        Assertions.assertEquals(2, field.getId());
        Assertions.assertEquals("name", field.getName());
        Assertions.assertEquals("char", field.getType());

        field = fields.get(2);
        Assertions.assertEquals(3, field.getId());
        Assertions.assertEquals("long", field.getType());
        Assertions.assertEquals("age", field.getName());
        Assertions.assertTrue(field.isIndexed());

        field = fields.get(3);
        Assertions.assertEquals(4, field.getId());
        Assertions.assertEquals("char", field.getType());
        Assertions.assertEquals("country_code", field.getName());
        Assertions.assertTrue(field.isIndexed());
        Assertions.assertTrue(field.getIndex().isLowCardinality());

        // Todo: runtime exception should not get thrown
        Assertions.assertThrows(RuntimeException.class, () -> {
            new ModelToCollectionConverter(TestModel2.class).toCollection();
        });
    }

}
