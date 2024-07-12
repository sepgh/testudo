package com.github.sepgh.testudo.storage.index.header;

import java.nio.file.Path;

public interface IndexHeaderManagerFactory {
    IndexHeaderManager getInstance(Path path);
}
