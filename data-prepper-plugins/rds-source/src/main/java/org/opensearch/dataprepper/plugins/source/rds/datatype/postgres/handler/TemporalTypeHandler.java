package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.utils.PgArrayParser;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.util.Locale;

public class TemporalTypeHandler implements PostgresDataTypeHandler {

    private static final String POSTGRES_DATE_FORMAT = "yyyy-MM-dd";
    private static final String POSTGRES_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final String POSTGRES_DATE_MIN_INFINITY = "1970-01-01";
    private static final String POSTGRES_DATE_MAX_INFINITY = "9999-12-31";
    private static final String POSTGRES_TIMESTAMP_MIN_INFINITY = "1970-01-01 00:00:00";
    private static final String POSTGRES_TIMESTAMP_MAX_INFINITY = "9999-12-31 23:59:59";
    private static final String POSTGRES_TIMESTAMPTZ_MIN_INFINITY = "1970-01-01 00:00:00+00";
    private static final String POSTGRES_TIMESTAMPTZ_MAX_INFINITY = "9999-12-31 23:59:59+00";

    private static final String NEGATIVE_INFINITY = "-infinity";
    private static final String INFINITY = "infinity";

    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isTemporal()) {
            throw new IllegalArgumentException("ColumnType is not Temporal: " + columnType);
        }
        if(columnType.isSubCategoryArray())
            return PgArrayParser.parseTypedArray(value.toString(), PostgresDataType.getScalarType(columnType),
                    this::parseTemporalValue);
        return parseTemporalValue(columnType, value.toString());
    }

    private Object parseTemporalValue(PostgresDataType columnType, String stringVal) {
        final String val = (stringVal.equals(NEGATIVE_INFINITY) || stringVal.equals(INFINITY))
                ? parseInfinity(stringVal, columnType)
                : stringVal;
        switch (columnType) {
            case DATE:
                return handleDate(val);
            case TIME:
                return handleTime(val);
            case TIMETZ:
                return handleTimeWithTimeZone(val);
            case TIMESTAMP:
                return handleTimeStamp(val);
            case TIMESTAMPTZ:
                return handleTimeStampWithTimeZone(val);
            case INTERVAL:
                return handleInterval(val);
            default:
                return val;
        }
    }

    private String parseInfinity(String val, PostgresDataType columnType) {
        switch (columnType) {
            case DATE:
                return val.equals(NEGATIVE_INFINITY) ? POSTGRES_DATE_MIN_INFINITY : POSTGRES_DATE_MAX_INFINITY;
            case TIMESTAMP:
                return val.equals(NEGATIVE_INFINITY) ? POSTGRES_TIMESTAMP_MIN_INFINITY : POSTGRES_TIMESTAMP_MAX_INFINITY;
            case TIMESTAMPTZ:
                return val.equals(NEGATIVE_INFINITY) ? POSTGRES_TIMESTAMPTZ_MIN_INFINITY : POSTGRES_TIMESTAMPTZ_MAX_INFINITY;
            default:
                return val; // For other types, return the original value
        }
    }

    private DateTimeFormatter createDateFormatter() {
        return new DateTimeFormatterBuilder()
                .appendPattern(POSTGRES_DATE_FORMAT)
                .optionalStart()
                .appendLiteral(' ')
                .appendText(ChronoField.ERA, TextStyle.SHORT)
                .optionalEnd()
                .toFormatter(Locale.ENGLISH);
    }

    private DateTimeFormatter createTimeFormatter() {
        return new DateTimeFormatterBuilder()
                .appendPattern("HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .optionalEnd()
                .toFormatter();
    }

    private DateTimeFormatter createTimeWithTimeZoneFormatter() {
        return new DateTimeFormatterBuilder()
                .appendPattern("HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .optionalEnd()
                .appendOffset("+HH:mm:ss", "+HH")
                .toFormatter();
    }

    private DateTimeFormatter createTimeStampWithoutTimeZoneFormatter() {
        return new DateTimeFormatterBuilder()
                .appendPattern(POSTGRES_TIMESTAMP_FORMAT)
                .optionalStart()
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .optionalEnd()
                .toFormatter();
    }

    private DateTimeFormatter createTimeStampWithTimeZoneFormatter() {
        return new DateTimeFormatterBuilder()
                .appendPattern(POSTGRES_TIMESTAMP_FORMAT)
                .optionalStart()
                .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                .optionalEnd()
                .optionalStart()
                .appendOffset("+HH:mm:ss", "+HH")
                .optionalEnd()
                .toFormatter();
    }

    private Long handleDate(final String dateStr) {
        try {
            final LocalDate date = LocalDate.parse(dateStr, createDateFormatter());
            return date.atStartOfDay(ZoneOffset.UTC)
                    .toInstant()
                    .toEpochMilli();
        }
        catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid date format: " + dateStr, e);
        }
    }

    private Long handleTime(final String timeStr) {
        try {
            final LocalTime time = LocalTime.parse(timeStr, createTimeFormatter());
            return time.atDate(LocalDate.EPOCH)
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        }
        catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid time format: " + timeStr, e);
        }
    }

    private Long handleTimeWithTimeZone(final String timetzStr) {
        try {
            final OffsetTime time = OffsetTime.parse(timetzStr, createTimeWithTimeZoneFormatter());
            return time.atDate(LocalDate.EPOCH)
                    .toInstant()
                    .toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid time with timezone format: " + timetzStr, e);

        }
    }

    private Long handleTimeStamp(final String timeStampStr) {
        try {
            return LocalDateTime.parse(timeStampStr, createTimeStampWithoutTimeZoneFormatter())
                    .toInstant(ZoneOffset.UTC)
                    .toEpochMilli();
        }
        catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid timestamp format: " + timeStampStr, e);
        }
    }

    private Long handleTimeStampWithTimeZone(final String timeStampWithTimeZoneStr) {
        try {
            return OffsetDateTime.parse(timeStampWithTimeZoneStr, createTimeStampWithTimeZoneFormatter())
                    .toInstant()
                    .toEpochMilli();
        }
        catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid timestamp with timezone format: " + timeStampWithTimeZoneStr, e);
        }
    }

    private String handleInterval(final String intervalStr) {
        int years = 0, months = 0, days = 0, hours = 0, minutes = 0, seconds = 0;

        String[] parts = intervalStr.split(" ");
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].endsWith("year") || parts[i].endsWith("years")) {
                years = Integer.parseInt(parts[i-1]);
            } else if (parts[i].endsWith("mon") || parts[i].endsWith("mons")) {
                months = Integer.parseInt(parts[i-1]);
            } else if (parts[i].endsWith("day") || parts[i].endsWith("days")) {
                days = Integer.parseInt(parts[i-1]);
            } else if (parts[i].contains(":")) {
                String[] timeParts = parts[i].split(":");
                hours = Integer.parseInt(timeParts[0]);
                minutes = Integer.parseInt(timeParts[1]);
                seconds = Integer.parseInt(timeParts[2]);
                break;
            }
        }

        StringBuilder result = new StringBuilder("P");
        if (years > 0) result.append(years).append("Y");
        if (months > 0) result.append(months).append("M");
        if (days > 0) result.append(days).append("D");

        if (hours > 0 || minutes > 0 || seconds > 0) {
            result.append("T");
            if (hours > 0) result.append(hours).append("H");
            if (minutes > 0) result.append(minutes).append("M");
            if (seconds > 0) result.append(seconds).append("S");
        }
        return result.length() > 1 ? result.toString() : "PT0S";
    }

}
