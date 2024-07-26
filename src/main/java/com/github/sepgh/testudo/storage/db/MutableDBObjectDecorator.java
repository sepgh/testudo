package com.github.sepgh.testudo.storage.db;

import com.github.sepgh.testudo.exception.VerificationException;


public class MutableDBObjectDecorator extends DBObject {

    public MutableDBObjectDecorator(DBObject decorated) throws VerificationException.InvalidDBObjectWrapper {
        super(decorated.getPage(), decorated.getBegin(), decorated.getEnd());
    }

    public void modifyData(int offset, byte[] value){
        // todo: throw err
    }

    public void setCollectionId(int collectionId) {
        // todo: throw err

    }

    public void setSize(int size) {
        // todo: throw err

    }

    public void deactivate() {
        // todo: throw err
    }

}
