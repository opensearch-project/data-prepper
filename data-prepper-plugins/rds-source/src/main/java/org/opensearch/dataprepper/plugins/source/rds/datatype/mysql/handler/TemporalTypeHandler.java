package org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Handles MySQL temporal data types (DATE, TIME, DATETIME, TIMESTAMP, YEAR) conversion between binlog and S3 export formats.
 *
 * The BinlogClient is configured with EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG.
 * MySQL binlog temporal types are deserialized to use Unix time (milliseconds elapsed since 1970-01-01 00:00:00 UTC):
 * - DATE: long value representing milliseconds since epoch (1970-01-01)
 * - TIME: long value representing milliseconds since epoch (1970-01-01 00:00:00)
 * - DATETIME: long value representing milliseconds since epoch (1970-01-01 00:00:00)
 * - TIMESTAMP: long value representing milliseconds since epoch (1970-01-01 00:00:00)
 * - YEAR: 4-digit year value (Example: 2024)
 *
 * RDS S3 export formats:
 * - DATE: "yyyy-MM-dd" (Example: "2024-01-15")
 * - TIME: "HH:mm:ss[.SSSSSS]" (Example: "14:30:00.123456")
 * - DATETIME: "yyyy-MM-dd HH:mm:ss[.SSSSSS]" (Example: "2024-01-15 14:30:00.123456")
 * - TIMESTAMP: "yyyy-MM-dd HH:mm:ss[.SSSSSS]" (Example: "2024-01-15 14:30:00.123456")
 *   Note: Fractional seconds are optional for DATETIME and TIMESTAMP
 * - YEAR: "yyyy" (Example: "2024")
 */
public class TemporalTypeHandler implements MySQLDataTypeHandler {
    private static final String MYSQL_DATE_FORMAT = "yyyy-MM-dd";
    private static final String MYSQL_TIME_FORMAT = "HH:mm:ss[.SSSSSS]";
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
            final Long timeEpoch = parseDateTimeStrAsEpochMillis(timeStr);
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
            // Handle MySQL zero date special case
            if ("0000-00-00".equals(dateStr)) {
                return null;
            }

            // Try parsing as Unix timestamp first
            final Long dateEpoch = parseDateTimeStrAsEpochMillis(dateStr);
            if (dateEpoch != null) return dateEpoch;

            LocalDate date = LocalDate.parse(dateStr, DATE_FORMATTER);
            return date.atStartOfDay(ZoneOffset.UTC)
                    .toInstant()
                    .toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid date format: " + dateStr, e);
        }
    }

    private Long handleDateTime(final String dateTimeStr) {
        try {
            final Long dateTimeEpoch = parseDateTimeStrAsEpochMillis(dateTimeStr);
            if (dateTimeEpoch != null) return dateTimeEpoch;

            // Parse using standard MySQL datetime format
            return LocalDateTime.parse(dateTimeStr, DATETIME_FORMATTER)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid datetime format: " + dateTimeStr, e);
        }
    }

    // Binlog reader converts temporal fields to epoch millis
    // The Binlog reader is set with EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
    private Long parseDateTimeStrAsEpochMillis(final String dateTimeStr) {
        // Try parsing as Unix timestamp
        try {
            // Date Time field in OpenSearch when represented as long value should be in milliseconds since the epoch
            // If the value is already in microseconds (length > 13) convert to milliseconds
            if (dateTimeStr.length() > 13) {
                // Convert microseconds to milliseconds by dividing by 1000
                return Long.parseLong(dateTimeStr) / 1000;
            }


            return Long.parseLong(dateTimeStr);
        } catch (NumberFormatException ignored) {
            // Continue with datetime parsing
        }
        return null;
    }

    private Long handleYear(final String yearStr) {
        try {
            // MySQL YEAR values are typically four-digit numbers (e.g., 2024).
            final long year = Long.parseLong(yearStr);

            // MySQL converts values in 1- or 2-digit strings in the range '0' to '99' to YYYY format
            // MySQL YEAR values in YYYY format are with a range of 1901 to 2155. Outside this range the value is 0.
            if (year <= 1900 || year > 2155) {
                return 0L;
            }

            return year;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid year format: " + yearStr, e);
        }
    }
}
