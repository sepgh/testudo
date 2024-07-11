package com.github.sepgh.testudo.storage.header;

import java.nio.file.Path;

public interface IndexHeaderManagerFactory {
    IndexHeaderManager getInstance(Path path);
}
