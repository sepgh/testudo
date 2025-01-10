package com.github.sepgh.testudo.storage.pool;

import com.github.sepgh.testudo.exception.ErrorMessage;
import com.github.sepgh.testudo.exception.InternalOperationException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Getter
public class FileHandler {
    private static final Logger logger = LoggerFactory.getLogger(FileHandler.class);

    private final AsynchronousFileChannel fileChannel;
    private ExecutorService executor;
    private int usageCount = 0;
    private volatile boolean closed = Boolean.FALSE;

    public FileHandler(String filePath, ExecutorService executorService) throws IOException {
        this.fileChannel = AsynchronousFileChannel.open(Path.of(filePath),
                Set.of(
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE
                ),
                executorService
        );
        this.executor = executorService;
    }

    public FileHandler(String filePath) throws IOException {
        this.fileChannel = AsynchronousFileChannel.open(Path.of(filePath),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );
    }

    public synchronized void incrementUsage() {
        if (this.closed){
            throw new IllegalStateException("FileHandler has been closed or is closing.");
        }
        usageCount++;
    }

    public synchronized void decrementUsage() {
        if (--this.usageCount == 0){
            notifyAll();
        }
    }

    public void close(long timeout, TimeUnit timeUnit) throws InternalOperationException {
        this.closed = Boolean.TRUE;
        synchronized (this){
            try {
                while (usageCount > 0){
                    try {
                        wait(timeUnit.toMillis(timeout));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new InternalOperationException(ErrorMessage.EM_FILEHANDLER_CLOSE, e);
                    }
                }
            } finally {
                usageCount = 0;
                try {
                    fileChannel.close();
                } catch (IOException e){
                    logger.error("Failed to close file channel", e);
                }
            }


            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    public static class SingletonFileHandlerFactory implements FileHandlerFactory {
        private ExecutorService executorService;

        private static SingletonFileHandlerFactory FACTORY_INSTANCE;

        public static synchronized SingletonFileHandlerFactory getInstance(){
            return getInstance(null);
        }

        public static synchronized SingletonFileHandlerFactory getInstance(int threadCount){
            return getInstance(Executors.newFixedThreadPool(threadCount));
        }

        public static synchronized SingletonFileHandlerFactory getInstance(@Nullable ExecutorService executorService){
            if (FACTORY_INSTANCE == null){
                FACTORY_INSTANCE = new SingletonFileHandlerFactory();
                if (executorService != null){
                    FACTORY_INSTANCE.executorService = executorService;
                }
            }
            return FACTORY_INSTANCE;
        }

        private SingletonFileHandlerFactory(){}

        @Override
        public synchronized FileHandler getFileHandler(String path) throws IOException {
            if (executorService == null)
                return new FileHandler(path);
            return new FileHandler(path, executorService);
        }
    }

}
