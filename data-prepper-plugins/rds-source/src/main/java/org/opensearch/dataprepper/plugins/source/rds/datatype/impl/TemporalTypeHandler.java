package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Handles MySQL temporal data types (DATE, TIME, DATETIME, YEAR) conversion between binlog and S3 export formats.
 *
 * MySQL binlog represents temporal types as follows:
 * - DATE: long value representing days since epoch (1970-01-01)
 * - TIME: long value representing milliseconds since epoch (1970-01-01 00:00:00)
 * - DATETIME: long value representing microseconds since epoch (1970-01-01 00:00:00)
 * - YEAR: 4-digit year value (Example: 2024)
 *
 * S3 export formats:
 * - DATE: "yyyy-MM-dd" (Example: "2024-01-15")
 * - TIME: "HH:mm:ss" (Example: "14:30:00")
 * - DATETIME: "yyyy-MM-dd HH:mm:ss[.SSSSSS]" (Example: "2024-01-15 14:30:00.123456")
 *   Note: Fractional seconds are optional for DATETIME
 * - YEAR: "yyyy" (Example: "2024")
 */
public class TemporalTypeHandler implements DataTypeHandler {
    private static final String MYSQL_DATE_FORMAT = "yyyy-MM-dd";
    private static final String MYSQL_TIME_FORMAT = "HH:mm:ss";
    private static final String MYSQL_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss[.SSSSSS]";

    // Thread-safe formatters
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(MYSQL_DATE_FORMAT);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern(MYSQL_TIME_FORMAT);
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern(MYSQL_DATETIME_FORMAT);

    @Override
    public Long handle(final MySQLDataType columnType, final String columnName, final Object value,
                       final TableMetadata metadata) {
        if (value == null) {
            return null;
        }

        final String strValue = value.toString().trim();

        try {
            switch (columnType) {
                case DATE:
                    return handleDate(strValue);
                case TIME:
                    return handleTime(strValue);
                case DATETIME:
                case TIMESTAMP:
                    return handleDateTime(strValue);
                case YEAR:
                    return handleYear(strValue);
                default:
                    throw new IllegalArgumentException("Unsupported temporal data type: " + columnType);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Failed to parse %s value: %s", columnType, strValue), e);
        }
    }

    private Long handleTime(final String timeStr) {
        try {
            // Try parsing as Unix timestamp first
            final Long timeEpoch = parseDateTimeStrAsEpoch(timeStr);
            if (timeEpoch != null) return timeEpoch;

            final LocalTime time = LocalTime.parse(timeStr, TIME_FORMATTER);
            // Combine with date from EPOCH
            return time.atDate(LocalDate.EPOCH)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid time format: " + timeStr, e);
        }
    }

    private Long handleDate(final String dateStr) {
        try {
            // Try parsing as Unix timestamp first
            final Long dateEpoch = parseDateTimeStrAsEpoch(dateStr);
            if (dateEpoch != null) return dateEpoch;

            LocalDate date = LocalDate.parse(dateStr, DATE_FORMATTER);
            return date.atStartOfDay(ZoneId.systemDefault())
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toInstant()
                    .toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid date format: " + dateStr, e);
        }
    }

    private Long handleDateTime(final String dateTimeStr) {
        try {
            final Long dateTimeEpoch = parseDateTimeStrAsEpoch(dateTimeStr);
            if (dateTimeEpoch != null) return dateTimeEpoch;

            // Parse using standard MySQL datetime format
            return LocalDateTime.parse(dateTimeStr, DATETIME_FORMATTER)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid datetime format: " + dateTimeStr, e);
        }
    }

    private Long parseDateTimeStrAsEpoch(final String dateTimeStr) {
        // Try parsing as Unix timestamp first
        try {
            return Long.parseLong(dateTimeStr);
        } catch (NumberFormatException ignored) {
            // Continue with datetime parsing
        }
        return null;
    }

    private Long handleYear(final String yearStr) {
        try {
            // MySQL YEAR values are typically four-digit numbers (e.g., 2024).
            return Long.parseLong(yearStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid year format: " + yearStr, e);
        }
    }
}
