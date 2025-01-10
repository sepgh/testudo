package com.github.sepgh.testudo.index;

import com.github.sepgh.testudo.ds.Bitmap;
import com.github.sepgh.testudo.ds.Pointer;
import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.index.data.IndexBinaryObjectFactory;
import com.github.sepgh.testudo.operation.query.Order;
import com.github.sepgh.testudo.storage.db.DBObject;
import com.github.sepgh.testudo.storage.db.DatabaseStorageManager;
import com.github.sepgh.testudo.storage.index.header.IndexHeaderManager;
import com.github.sepgh.testudo.utils.IteratorUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import static com.github.sepgh.testudo.exception.ErrorMessage.EM_INDEX_HEADER_MANAGEMENT;


@AllArgsConstructor
public class NullableIndexManager<V extends Number & Comparable<V>> {
    private static final Logger logger = LoggerFactory.getLogger(NullableIndexManager.class);

    private final DatabaseStorageManager storageManager;
    private final IndexHeaderManager indexHeaderManager;
    private final IndexBinaryObjectFactory<V> vIndexBinaryObjectFactory;
    @Getter
    private final int indexId;

    public void addNull(V value) throws InternalOperationException {
        Optional<IndexHeaderManager.Location> optionalLocation = this.indexHeaderManager.getNullBitmapLocation(this.getIndexId());

        Pointer pointer = null;
        byte[] bytes;
        int initialSize = 0;
        boolean exists = false;

        if (optionalLocation.isPresent()) {
            pointer = optionalLocation.get().toPointer(Pointer.TYPE_DATA);

            Optional<DBObject> dbObjectOptional = this.storageManager.select(pointer);
            if (dbObjectOptional.isPresent()) {
                DBObject dbObject = dbObjectOptional.get();
                initialSize = dbObject.getDataSize();
                bytes = dbObject.getData();
                exists = true;
            } else {
                bytes = new byte[0];
            }
        } else {
            bytes = new byte[0];
        }

        Bitmap<V> bitmap = new Bitmap<>(vIndexBinaryObjectFactory.getType(), bytes);
        bitmap.on(value);

        byte[] data = bitmap.getData();

        if (!exists || initialSize < data.length) {
            try {
                Pointer storedPointer = this.storageManager.store(-1, getIndexId(), 1, data);// Todo: scheme id
                this.indexHeaderManager.setNullBitmapLocation(
                        getIndexId(),
                        IndexHeaderManager.Location.fromPointer(storedPointer)
                );
            } catch (IOException e) {
                throw new InternalOperationException(EM_INDEX_HEADER_MANAGEMENT, e); // Todo
            }

            if (pointer != null) {
                try {
                    this.storageManager.remove(pointer);
                } catch (InternalOperationException ignored) {
                    // ignoring errors, remove failure is not important in this case (todo: good idea?)
                }
            }
        } else {
            this.storageManager.update(pointer, data);
        }
    }

    public void removeNull(V value) throws InternalOperationException {
        Optional<IndexHeaderManager.Location> optionalLocation = this.indexHeaderManager.getNullBitmapLocation(this.getIndexId());
        if (optionalLocation.isEmpty()){
            return;
        }

        Pointer pointer = optionalLocation.get().toPointer(Pointer.TYPE_DATA);
        Optional<DBObject> dbObjectOptional = this.storageManager.select(pointer);
        if (dbObjectOptional.isEmpty()) {
            return;
        }

        DBObject dbObject = dbObjectOptional.get();
        Bitmap<V> bitmap = new Bitmap<>(vIndexBinaryObjectFactory.getType(), dbObject.getData());
        bitmap.off(value);

        if (dbObject.getDataSize() < bitmap.getData().length) {
            try {
                Pointer storedPointer = this.storageManager.store(-1, getIndexId(), 1, bitmap.getData());// Todo: scheme id
                this.indexHeaderManager.setNullBitmapLocation(
                        getIndexId(),
                        IndexHeaderManager.Location.fromPointer(storedPointer)
                );
            } catch (IOException e) {
                throw new InternalOperationException(EM_INDEX_HEADER_MANAGEMENT, e); // Todo
            } finally {
                try {
                    this.storageManager.remove(pointer);
                } catch (InternalOperationException ignored) {
                    // ignoring errors, remove failure is not important in this case (todo: good idea?)
                }
            }
        } else {
            this.storageManager.update(pointer, bitmap.getData());
        }

    }

    protected Optional<Bitmap<V>> getBitmap() {
        Optional<IndexHeaderManager.Location> optionalLocation = this.indexHeaderManager.getNullBitmapLocation(this.getIndexId());
        if (optionalLocation.isEmpty()){
            return Optional.empty();
        }

        Pointer pointer = optionalLocation.get().toPointer(Pointer.TYPE_DATA);
        Optional<DBObject> dbObjectOptional = null;
        try {
            dbObjectOptional = this.storageManager.select(pointer);
        } catch (InternalOperationException e) {
            logger.error(e.getMessage(), e);
            return Optional.empty();
        }
        if (dbObjectOptional.isEmpty()) {
            return Optional.empty();
        }

        DBObject dbObject = dbObjectOptional.get();
        return Optional.of(new Bitmap<>(vIndexBinaryObjectFactory.getType(), dbObject.getData()));
    }

    protected Iterator<V> getBitmapIterator(Order order, boolean on) {
        Optional<Bitmap<V>> optionalVBitmap = getBitmap();
        if (optionalVBitmap.isPresent()) {
            Bitmap<V> bitmap = optionalVBitmap.get();
            return on ? bitmap.getOnIterator(order) : bitmap.getOffIterator(order);
        }
        return IteratorUtils.getCleanIterator();
    }

    public boolean isNull(V value) {
        Optional<Bitmap<V>> optionalVBitmap = getBitmap();
        if (optionalVBitmap.isPresent()) {
            Bitmap<V> bitmap = optionalVBitmap.get();
            return bitmap.isOn(value);
        }
        return false;
    }

    public Iterator<V> getNulls(Order order) {
        return getBitmapIterator(order, true);
    }

    public Iterator<V> getNotNulls(Order order) {
        return getBitmapIterator(order, false);
    }

}
