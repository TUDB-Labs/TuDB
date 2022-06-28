/**
 * Copyright (c) 2022 TuDB
 **/
package org.grapheco.tudb.exception;

/**
 * @author : johnny
 * @date : 2022/6/24
 **/
public class SystemException extends TuDBException {
    /**
     *
     * @param code
     * @param message
     */
    public SystemException(TuDBErrorCode code, String message) {
        super(code, message);
    }

    /**
     *
     * @param code
     * @param message
     * @param throwable
     */
    public SystemException(TuDBErrorCode code, String message, Throwable throwable) {
        super(code, message, throwable);
    }
}
