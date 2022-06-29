/**
 * Copyright (c) 2022 TuDB
 **/
package org.grapheco.tudb.exception;

/**
 * exception in storage
 *
 * @author : johnny
 * @date : 2022/6/24
 **/
public class StorageException extends TuDBException {
    /**
     * constructor with code and message
     * @param code
     * @param message
     */
    public StorageException(TuDBError code, String message) {
        super(code, message);
    }

    /**
     * constructor with code and message and throwable
     * @param code
     * @param message
     * @param throwable
     */
    public StorageException(TuDBError code, String message, Throwable throwable) {
        super(code, message, throwable);
    }
}
