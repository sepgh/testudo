package com.github.sepgh.internal.storage.header;

import java.io.IOException;

public interface HeaderManager {
    Header getHeader();
    void update(Header header) throws IOException;
    void update() throws IOException;
}
