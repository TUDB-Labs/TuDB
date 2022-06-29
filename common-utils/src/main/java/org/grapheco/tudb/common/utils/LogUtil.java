/**
 * Copyright (c) 2022 TuDB
 **/
package org.grapheco.tudb.common.utils;

import org.slf4j.Logger;

/**
 * Commonly used standard logging util
 * In TuDB project ,if you want to print log ,whatever it in consoles or log file
 * you need coding like present :
 * LogUtil.info()
 * LogUtil.debug()
 * LogUtil.warn()
 * LogUtil.error()
 *
 * @author : johnny
 * @date : 2022/6/17
 **/
public class LogUtil {

    /**
     * print info level log
     * in production env , it will not be printed
     *
     * @param logger slf4j logger
     * @param format the msg you want format, ex. : 'a=%s , b=%d'
     * @param args   the values you want print to format schema
     */
    public static final void info(Logger logger, String format, Object... args) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format(format, args));
        }
    }

    /**
     * print debug level log
     *
     * @param logger
     * @param format
     * @param args
     */
    public static final void debug(Logger logger, String format, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format(format, args));
        }
    }

    /**
     * print warn level log
     *
     * @param logger
     * @param format
     * @param args
     */
    public static final void warn(Logger logger, String format, Object... args) {
        if (logger.isWarnEnabled()) {
            logger.warn(String.format(format, args));
        }
    }

    /**
     * print error level log
     *
     * @param logger
     * @param e      exception for error
     * @param format
     * @param args
     */
    public static final void error(Logger logger, Throwable e, String format, Object... args) {
        logger.error(String.format(format, args), e);
    }

}
