package com.github.sepgh.internal.storage.header;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

public class JsonHeaderManager implements HeaderManager {
    private static final Type HEADER_TYPE = new TypeToken<Header>() {}.getType();

    private final Gson gson;
    private final Path path;

    private final AtomicReference<Header> headerAtomicReference = new AtomicReference<>();

    public JsonHeaderManager(Path path) throws FileNotFoundException {
        this.path = path;
        gson = new Gson();
        initialize();
    }

    private void initialize() throws FileNotFoundException {
        JsonReader jsonReader = new JsonReader(new FileReader(this.path.toFile()));
        headerAtomicReference.set(
                this.gson.fromJson(jsonReader, HEADER_TYPE)
        );
    }

    public Header getHeader(){
        return headerAtomicReference.get();
    }

    public synchronized void update(Header header) throws IOException {
        gson.toJson(header, new FileWriter(this.path.toFile()));
        headerAtomicReference.set(header);
    }

    @Override
    public void update() throws IOException {
        gson.toJson(headerAtomicReference.get(), new FileWriter(this.path.toFile()));
    }
}
