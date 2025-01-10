package com.github.sepgh.testudo.functional;

import com.github.sepgh.testudo.exception.InternalOperationException;
import com.github.sepgh.testudo.storage.db.Page;
import com.github.sepgh.testudo.storage.db.PageBuffer;

@FunctionalInterface
public interface PageFactory {
    Page apply(PageBuffer.PageTitle t) throws InternalOperationException;
}
