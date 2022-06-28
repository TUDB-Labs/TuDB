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
     * @param code
     * @param message
     */
    public StorageException(TuDBErrorCode code, String message) {
        super(code, message);
    }

    /**
     *
     * @param code
     * @param message
     * @param throwable
     */
    public StorageException(TuDBErrorCode code, String message, Throwable throwable) {
        super(code, message, throwable);
    }
}
