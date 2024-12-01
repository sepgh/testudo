package com.github.sepgh.testudo.storage.index.header;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import lombok.SneakyThrows;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

public class JsonIndexHeaderManager extends InMemoryIndexHeaderManager {

    private final Path path;
    private final Header header;
    private final Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setPrettyPrinting().serializeNulls().create();

    public JsonIndexHeaderManager(Path path) throws IOException {
        Header Header1;
        this.path = path;

        Header readObject;
        try (JsonReader jsonReader = new JsonReader(new FileReader(path.toFile()))) {
            readObject = gson.fromJson(jsonReader, Header.class);
            Header1 = readObject;
        } catch (FileNotFoundException e) {
            Header1 = new Header();
            this.write();
        }

        this.header = Header1;
    }

    private synchronized void write() throws IOException {
        try (FileWriter writer = new FileWriter(this.path.toFile())) {
            gson.toJson(this.header, writer);
            writer.flush();
        } catch (FileNotFoundException e) {
            if (!this.path.toFile().createNewFile()) {
                throw e;
            }
            this.write();
        }
    }

    @Override
    public synchronized void setRootOfIndex(int indexId, Location location) throws IOException {
        super.setRootOfIndex(indexId, location);
        this.write();
    }

    @Override
    public synchronized void setIndexBeginningInChunk(int indexId, Location location) throws IOException {
        super.setIndexBeginningInChunk(indexId, location);
        this.write();
    }

    public static class Factory implements IndexHeaderManagerFactory {

        @Override
        @SneakyThrows
        public IndexHeaderManager getInstance(Path path) {
            return new JsonIndexHeaderManager(path);
        }
    }

}
