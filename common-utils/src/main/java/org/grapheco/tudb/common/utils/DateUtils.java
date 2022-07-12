/**
 * Copyright (c) 2022 TuDB
 **/
package org.grapheco.tudb.common.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author : johnny
 * @date : 2022/7/5
 **/
public class DateUtils {

    /**
     * parse date str to Date object
     * only parse format style like yyyy-MM-dd HH:mm:ss
     * @param dateStr "2001-01-01 00:00:00"
     * @return java.util.Date object
     * @throws ParseException
     */
    public static Date parseSimpleDate(String dateStr) throws ParseException {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return fmt.parse(dateStr);
    }
}
