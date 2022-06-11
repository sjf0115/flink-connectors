package com.flink.format.debezium.common;

import org.apache.flink.annotation.Internal;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * 功能：TimeFormats
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/6/8 上午9:03
 */
@Internal
public class TimeFormats {
    /** Formatter for RFC 3339-compliant string representation of a time value. */
    public static final DateTimeFormatter RFC3339_TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .appendPattern("'Z'")
                    .toFormatter();

    /**
     * Formatter for RFC 3339-compliant string representation of a timestamp value (with UTC
     * timezone).
     */
    public static final DateTimeFormatter RFC3339_TIMESTAMP_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral('T')
                    .append(RFC3339_TIME_FORMAT)
                    .toFormatter();

    /** Formatter for ISO8601 string representation of a timestamp value (without UTC timezone). */
    public static final DateTimeFormatter ISO8601_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    /** Formatter for SQL string representation of a time value. */
    public static final DateTimeFormatter SQL_TIME_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    /** Formatter for SQL string representation of a timestamp value (without UTC timezone). */
    public static final DateTimeFormatter SQL_TIMESTAMP_FORMAT =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(SQL_TIME_FORMAT)
                    .toFormatter();

    private TimeFormats() {}
}
