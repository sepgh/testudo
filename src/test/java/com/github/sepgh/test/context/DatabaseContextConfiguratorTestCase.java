package com.github.sepgh.test.context;

import com.github.sepgh.test.utils.FileUtils;
import com.github.sepgh.testudo.context.DatabaseContext;
import com.github.sepgh.testudo.context.DatabaseContextConfigurator;
import com.github.sepgh.testudo.context.EngineConfig;
import com.github.sepgh.testudo.exception.BaseSerializationException;
import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.operation.CollectionOperation;
import com.github.sepgh.testudo.operation.query.Operation;
import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.scheme.ModelToCollectionConverter;
import com.github.sepgh.testudo.scheme.Scheme;
import com.github.sepgh.testudo.scheme.annotation.Collection;
import com.github.sepgh.testudo.scheme.annotation.Field;
import com.github.sepgh.testudo.scheme.annotation.Index;
import lombok.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class DatabaseContextConfiguratorTestCase {
    private DatabaseContextConfigurator configurator;
    private Path dbPath;

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

    @BeforeEach
    public void setup() throws IOException {
        this.dbPath = Files.createTempDirectory(this.getClass().getSimpleName());

        this.configurator = new DatabaseContextConfigurator() {

            @Override
            public EngineConfig engineConfig() {
                return EngineConfig.builder()
                        .clusterKeyType(EngineConfig.ClusterKeyType.INT)
                        .baseDBPath(dbPath.toString())
                        .build();
            }

            @Override
            public Scheme scheme() {
                Scheme.Collection collection = new ModelToCollectionConverter(TestModel.class).toCollection();
                return Scheme.builder().version(1).dbName("test").collections(Collections.singletonList(collection)).build();
            }
        };
    }

    @AfterEach
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(dbPath.toString());
    }


    @Test
    public void operationsFromContext() throws BaseSerializationException, InternalOperationException {
        DatabaseContext databaseContext = this.configurator.databaseContext();

        CollectionOperation collectionOperation = databaseContext.getOperation("test");

        TestModel sample = TestModel.builder()
                .id(1)
                .name("John")
                .age(10L)
                .country("DE")
                .build();

        collectionOperation.insert().execute(sample);

        Query query = new Query("country_code", Operation.EQ, "DE");
        long count = collectionOperation.select().query(query).count();
        Assertions.assertEquals(1L, count);

        List<TestModel> list = collectionOperation.select().asList(TestModel.class);
        Assertions.assertEquals(1L, list.size());
        Assertions.assertEquals(sample, list.getFirst());

        long updateCount = collectionOperation.update().query(query).execute(model -> {
            model.setCountry("FR");
        }, TestModel.class);
        Assertions.assertEquals(1L, updateCount);

        query = new Query("country_code", Operation.EQ, "FR");
        count = collectionOperation.select().query(query).count();
        Assertions.assertEquals(1L, count);

        long deletedCount = collectionOperation.delete().query(query).execute();
        Assertions.assertEquals(1, deletedCount);
    }


}
