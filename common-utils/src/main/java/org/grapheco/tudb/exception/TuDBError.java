package org.grapheco.tudb.exception;

/**
 * error code enums
 *
 * @author : johnny
 * @date : 2022/6/23
 **/
public enum TuDBError {

    /**
     * ========================
     * reserve code 0000 - 0999
     * lynx-cypher error code  1000 - 1999
     * storage error code 2000 - 2999
     * query-engine error code 3000 - 3999
     * tu-server error code 4000-4999
     * client error code 9000-9999
     */
    /**
     * reserve
     */
    UNKNOWN_ERROR("0000", "UNKNOWN_EXCEPTION"),
    SYSTEM_ERROR("0001", "SYSTEM_ERROR"),

    /**
     * lynx
     */
    LYNX_CYPHER_ERROR("1000", "LYNX_CYPHER_ERROR"),
    LYNX_PARSING_ERROR("1001","LYNX_PARSING_ERROR"),
    LYNX_PROCEDURE_UNREGISTERED_ERROR("1002","LYNX_PARSING_ERROR"),
    LYNX_NO_INDEX_MANAGER_ERROR("1003","LYNX_PARSING_ERROR"),
    LYNX_CONSTRAIN_VIOLATED_ERROR("1004","LYNX_PARSING_ERROR"),
    LYNX_TEMPORAL_PARSE_ERROR("1005","LYNX_TEMPORAL_PARSE_ERROR"),
    LYNX_PROCEDURE_ERROR("1006","LYNX_PROCEDURE_ERROR"),
    LYNX_UNKNOWN_PROCEDURE("1007","LYNX_UNKNOWN_PROCEDURE"),
    LYNX_WRONG_ARGUMENT("1008","LYNX_WRONG_ARGUMENT"),
    LYNX_WRONG_NUMBER_OF_ARGUMENT("1009","LYNX_WRONG_NUMBER_OF_ARGUMENT"),
    LYNX_RESULT_EMPTY("1010","LYNX_RESULT_EMPTY"),
    LYNX_UNSUPPORTED_OPERATION("1011", "LYNX_UNSUPPORTED_OPERATION"),




    /**
     * storage
     */
    STORAGE_ERROR("2000","STORAGE_ERROR"),
    /**
     * client
     */
    CLIENT_ERROR("9000","CLIENT_ERROR")
    ;

    /**
     * error code number
     **/
    private String code;
    /**
     * error code description
     */
    private String description;

    /**
     * construction
     *
     * @param code
     * @param description
     */
    TuDBError(String code, String description) {
        this.code = code;
        this.description = description;
    }
    }
