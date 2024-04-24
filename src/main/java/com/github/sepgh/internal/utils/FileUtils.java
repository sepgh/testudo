package com.github.sepgh.internal.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class FileUtils {
    public static Future<byte[]> readBytes(AsynchronousFileChannel asynchronousFileChannel, long position, int size){
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        asynchronousFileChannel.read(
                buffer,
                position,
                buffer, new CompletionHandler<Integer, ByteBuffer>() {
                    @Override
                    public void completed(Integer result, ByteBuffer attachment) {
                        attachment.flip();
                        byte[] data = new byte[attachment.limit()];
                        attachment.get(data);
                        future.complete(data);
                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer attachment) {
                        attachment.flip();
                        future.completeExceptionally(exc);
                    }
                }
        );
        return future;
    }


    public static Future<Long> allocate(AsynchronousFileChannel asynchronousFileChannel, int size) throws IOException {
        CompletableFuture<Long> future = new CompletableFuture<>();
        long fileSize = asynchronousFileChannel.size();
        asynchronousFileChannel.write(ByteBuffer.allocate(size), fileSize, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                try {
                    asynchronousFileChannel.force(true);
                    future.complete(fileSize);
                } catch (IOException e) {
                    future.completeExceptionally(e);
                }
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

    public static Future<Long> allocate(AsynchronousFileChannel asynchronousFileChannel, long position, int size) throws IOException {
        CompletableFuture<Long> future = new CompletableFuture<>();

        int capacity = (int) Math.subtractExact(
                asynchronousFileChannel.size(),
                position
        );

        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        asynchronousFileChannel.read(buffer, position, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                buffer.flip();
                ByteBuffer dataAfterPosition = ByteBuffer.allocate(capacity);
                dataAfterPosition.put(buffer);

                // Write the empty area at the desired position
                ByteBuffer emptyBuffer = ByteBuffer.allocate((int) size);
                asynchronousFileChannel.write(emptyBuffer, position, null, new CompletionHandler<Integer, Object>() {
                    @Override
                    public void completed(Integer result, Object attachment) {
                        asynchronousFileChannel.write(dataAfterPosition, position + size, null, new CompletionHandler<Integer, Object>() {
                            @Override
                            public void completed(Integer result, Object attachment) {
                                try {
                                    asynchronousFileChannel.force(true);
                                    future.complete(position);
                                } catch (IOException e){
                                    future.completeExceptionally(e);
                                }
                            }

                            @Override
                            public void failed(Throwable exc, Object attachment) {
                                future.completeExceptionally(exc);
                            }
                        });
                    }

                    @Override
                    public void failed(Throwable exc, Object attachment) {
                        future.completeExceptionally(exc);
                    }
                });
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                future.completeExceptionally(exc);
            }
        });

        return future;
    }

    public static Future<Integer> write(AsynchronousFileChannel asynchronousFileChannel, long position, byte[] content){
        CompletableFuture<Integer> future = new CompletableFuture<>();

        ByteBuffer byteBuffer = ByteBuffer.allocate(content.length);
        byteBuffer.put(content);
        byteBuffer.flip();

        asynchronousFileChannel.write(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                future.complete(result);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                future.completeExceptionally(exc);
            }
        });

        return future;

    }

}
