package com.github.sepgh.testudo.exception;

public interface ErrorMessage {
    String EM_FILEHANDLER_POOL = "Could not receive file channel from file handler pool";
    String EM_FILEHANDLER_CLOSE = "Could not close file channel";
    String EM_FILEHANDLER_CREATE = "Could not create file channel";
    String EM_FILE_WRITE = "Failed to write to file";
    String EM_FILE_ALLOCATION = "Failed to allocate space in file";
    String EM_FILE_READ_EMPTY = "Nothing available to read";
    String EM_INDEX_HEADER_MANAGEMENT = "Failed to manage index header";
}
