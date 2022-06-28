/**
 * Copyright (c) 2022 TuDB
 **/
package org.grapheco.tudb.exception;

/**
 * client errors
 *
 * @author : johnny
 * @date : 2022/6/24
 **/
public class ClientException extends TuDBException {
    /**
     *
     * @param code
     * @param message
     */
    public ClientException(TuDBErrorCode code, String message) {
        super(code, message);
    }

    /**
     *
     * @param code
     * @param message
     * @param throwable
     */
    public ClientException(TuDBErrorCode code, String message, Throwable throwable) {
        super(code, message, throwable);
    }
}
