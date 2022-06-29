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
    private TuDBError code;

    /**
     * default construction method
     *
     * @param code
     * @param message
     */
    public TuDBException(TuDBError code, String message) {
        super(message);
        this.code = code;
    }

    /**
     * construction method with throwable
     *
     * @param message
     * @param throwable
     */
    public TuDBException(TuDBError code, String message, Throwable throwable) {
        super(message, throwable);
        this.code = code;
    }

    public TuDBError getCode() {
        return code;
    }
}
