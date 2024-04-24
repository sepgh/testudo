package com.github.sepgh.internal.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class HeaderReader {
    public Future<byte[]> readUntilChar(AsynchronousFileChannel channel, char specialChar) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        ByteBuffer buffer = ByteBuffer.allocate(1024); // Initial buffer size
        readData(channel, 0, buffer, specialChar, future);
        return future;
    }

    private void readData(AsynchronousFileChannel fileChannel, long position, ByteBuffer buffer, char specialChar, CompletableFuture<byte[]> future) {
        fileChannel.read(buffer, position, buffer, new CompletionHandler<Integer, ByteBuffer>() {
            @Override
            public void completed(Integer bytesRead, ByteBuffer attachment) {
                attachment.flip();

                while (attachment.hasRemaining()) {
                    if (attachment.get() == (byte) specialChar) {
                        int dataSize = attachment.position() - 1; // Exclude the special character
                        byte[] data = new byte[dataSize];
                        attachment.flip();
                        attachment.get(data);

                        future.complete(data);
                        try {
                            fileChannel.close();
                        } catch (IOException e) {
                            future.completeExceptionally(e);
                        }
                        return;
                    }
                }

                if (bytesRead == -1) {
                    // Special character not found in the file
                    future.complete(null);
                    try {
                        fileChannel.close();
                    } catch (IOException e) {
                        future.completeExceptionally(e);
                    }
                } else {
                    buffer.clear();
                    readData(fileChannel, position + bytesRead, buffer, specialChar, future); // Continue reading
                }
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                future.completeExceptionally(exc);
            }
        });
    }
}
