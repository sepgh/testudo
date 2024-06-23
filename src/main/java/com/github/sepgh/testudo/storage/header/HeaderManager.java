package com.github.sepgh.testudo.storage.header;

import java.io.IOException;

public interface HeaderManager {
    Header getHeader();
    void update(Header header) throws IOException;
    void update() throws IOException;
}
