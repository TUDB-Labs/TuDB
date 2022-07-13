/**
 * Copyright (c) 2022 TuDB
 **/
package com.grapheco.common.utils.test;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

/**
 * test for datetime utils
 *
 * @author : johnny
 * @date : 2022/7/11
 **/
public class DateTimeUtilsTest {

    @Test
    public void testDateTimeRegex() {
        String sdf = "\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{4}";
        String time = "2015-06-24 12:50:35 0800";
        Assert.assertTrue(Pattern.matches(sdf, time));

        String time1 = "2002-02-02T02:02:02 0800";
        Assert.assertFalse(Pattern.matches(sdf, time1));

        String zdf = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}[.]\\d{3}[+]\\d{2}[:]\\d{2}";
        String ztime = "2015-06-24T12:50:35.123+08:00";
        Assert.assertTrue(Pattern.matches(zdf, ztime));
    }
}
