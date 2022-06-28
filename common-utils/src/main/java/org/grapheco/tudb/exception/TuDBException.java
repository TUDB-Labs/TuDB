/**
 * Copyright (c) 2022 TuDB
 **/
package org.grapheco.tudb.exception;

/**
 * TuDB Base Exception
 * All TuDB's customized Exception must extend TuDBException
 *
 * @author : johnny
 * @date : 2022/6/23
 */
public class TuDBException extends RuntimeException {

    /**
     * error code
     */
    private TuDBErrorCode code;

    /**
     * default construction method
     *
     * @param code
     * @param message
     */
    public TuDBException(TuDBErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    /**
     * construction method with throwable
     *
     * @param message
     * @param throwable
     */
    public TuDBException(TuDBErrorCode code, String message, Throwable throwable) {
        super(message, throwable);
        this.code = code;
    }

    public TuDBErrorCode getCode() {
        return code;
    }
}
