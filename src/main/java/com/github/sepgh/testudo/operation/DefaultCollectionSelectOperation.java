package com.github.sepgh.testudo.operation;

import com.github.sepgh.testudo.exception.DeserializationException;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.exception.SerializationException;
import com.github.sepgh.testudo.index.Pointer;
import com.github.sepgh.testudo.index.UniqueTreeIndexManager;
import com.github.sepgh.testudo.operation.query.Query;
import com.github.sepgh.testudo.serialization.ModelDeserializer;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.utils.IteratorUtils;
import lombok.Getter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class DefaultCollectionSelectOperation implements CollectionSelectOperation {

    private final CollectionIndexProvider collectionIndexProvider;
    private final DatabaseStorageManager storageManager;
    @Getter
    private Query query;
    private List<String> fields;

    public DefaultCollectionSelectOperation(CollectionIndexProvider collectionIndexProvider, DatabaseStorageManager storageManager) {
        this.collectionIndexProvider = collectionIndexProvider;
        this.storageManager = storageManager;
    }


    // Todo: implement the actual field limitations
    @Override
    public CollectionSelectOperation fields(String... fields) {
        this.fields = Arrays.asList(fields);
        return this;
    }

    @Override
    public CollectionSelectOperation query(Query query) {
        this.query = query;
        return this;
    }

    protected <V extends Number & Comparable<V>> Iterator<V> getExecutedQuery() {
        if (query == null) {
            this.query = new Query();
        }
        return query.execute(this.collectionIndexProvider);
    }

    @Override
    public <V extends Number & Comparable<V>> Iterator<DBObject> execute() {
        Iterator<V> executedQuery = getExecutedQuery();

        UniqueTreeIndexManager<V, Pointer> clusterIndexManager = (UniqueTreeIndexManager<V, Pointer>) this.collectionIndexProvider.getClusterIndexManager();

        return IteratorUtils.modifyNext(
                executedQuery,
                i -> {
                    try {
                        Optional<Pointer> optionalPointer = clusterIndexManager.getIndex(i);
                        if (optionalPointer.isPresent()) {
                            Optional<DBObject> dbObjectOptional = this.storageManager.select(optionalPointer.get());
                            if (dbObjectOptional.isPresent()) {
                                return dbObjectOptional.get();
                            }
                        }
                        throw new RuntimeException("Could not read object");  // Todo
                    } catch (InternalOperationException e) {
                        throw new RuntimeException(e); // Todo
                    }
                }
        );

    }

    @Override
    public <V extends Number & Comparable<V>, T> Iterator<T> execute(Class<T> clazz) {
        ModelDeserializer<T> modelDeserializer = new ModelDeserializer<>(clazz);
        return IteratorUtils.modifyNext(execute(), dbObject -> {
            try {
                return modelDeserializer.deserialize(dbObject.getData());
            } catch (SerializationException | DeserializationException e) {
                throw new RuntimeException(e); // Todo
            }
        });
    }

    @Override
    public long count() {
        long count = 0;
        Iterator<?> executedQuery = getExecutedQuery();
        while (executedQuery.hasNext()) {
            count++;
            executedQuery.next();
        }
        return count;
    }
}
