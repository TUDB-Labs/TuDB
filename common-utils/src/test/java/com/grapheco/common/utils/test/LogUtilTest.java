/**
 * Copyright (c) 2022 TuDB
 **/
package com.grapheco.common.utils.test;

import org.grapheco.tudb.common.utils.LogUtil;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for LogUtil
 *
 * @author : johnny
 * @date : 2022/6/17
 **/
public class LogUtilTest {

    /**
     * logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(LogUtilTest.class);

    /**
     * test  log print
     */
    @Test
    public void testLog() {
        LogUtil.info(LOGGER, "this is my info log ,content is %s", "content1");
        LogUtil.debug(LOGGER, "this is my debug log ,content is %s", "content1");
        LogUtil.warn(LOGGER, "this is my warn log ,content is %s", "content1");

        Exception e = new RuntimeException();
        LogUtil.error(LOGGER, e, "this is my error log ,content is %s", "content1");
    }

}
