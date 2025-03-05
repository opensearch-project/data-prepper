package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;

import java.util.HashMap;
import java.util.Map;

public class RangeTypeHandler implements PostgresDataTypeHandler {

    public static final String EMPTY = "empty";
    public static final String GREATER_THAN = "gt";
    public static final String GREATER_THAN_OR_EQUAL_TO = "gte";
    public static final String LESSER_THAN = "lt";
    public static final String LESSER_THAN_OR_EQUAL_TO = "lte";

    private final NumericTypeHandler numericTypeHandler;
    private final TemporalTypeHandler temporalTypeHandler;

    public RangeTypeHandler(NumericTypeHandler numericTypeHandler, TemporalTypeHandler temporalTypeHandler) {
        this.numericTypeHandler = numericTypeHandler;
        this.temporalTypeHandler = temporalTypeHandler;
    }

    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isRange()) {
            throw new IllegalArgumentException("ColumnType is not range: " + columnType);
        }

        return parseRangeValue(columnType, columnName, value.toString());
    }

    private Object parseRangeValue(PostgresDataType columnType,String columnName, String rangeString) {

        if(rangeString.equals(EMPTY))
            return null;

        String cleanRangeString = rangeString.substring(1, rangeString.length() - 1);
        String[] rangeValues = cleanRangeString.split(",",-1);

        if (rangeValues.length == 2)  {
            String lowerBound = trimQuotes(rangeValues[0]);
            String upperBound = trimQuotes(rangeValues[1]);
            String lowerBoundInclusivity = String.valueOf(rangeString.charAt(0));
            String upperBoundInclusivity = String.valueOf(rangeString.charAt(rangeString.length() - 1));
            switch (columnType) {
                case INT4RANGE:
                    return handleNumericRange(PostgresDataType.INTEGER, columnName, lowerBoundInclusivity, lowerBound, upperBound, upperBoundInclusivity);
                case INT8RANGE:
                    return handleNumericRange(PostgresDataType.BIGINT, columnName, lowerBoundInclusivity, lowerBound, upperBound, upperBoundInclusivity);
               case TSRANGE:
                    return handleTemporalRange(PostgresDataType.TIMESTAMP, columnName, lowerBoundInclusivity, lowerBound, upperBound, upperBoundInclusivity);
                case TSTZRANGE:
                    return handleTemporalRange(PostgresDataType.TIMESTAMPTZ, columnName, lowerBoundInclusivity, lowerBound, upperBound, upperBoundInclusivity);
                case DATERANGE:
                    return handleTemporalRange(PostgresDataType.DATE, columnName, lowerBoundInclusivity, lowerBound, upperBound, upperBoundInclusivity);
                default:
                    throw new IllegalArgumentException("Unsupported range type: " + columnType);
            }
        } else {
            throw new IllegalArgumentException("Invalid range format: " + rangeString);
        }

    }
    private Map<String, Object> handleNumericRange(PostgresDataType columnType, String columnName, String lowerBoundInclusivity, String lowerBound, String upperBound, String upperBoundInclusivity) {
        Map<String, Object> rangeMap = new HashMap<>();

        if(!lowerBound.isEmpty()) {
            Object parsedLowerBound = numericTypeHandler.handle(columnType, columnName, lowerBound);
            rangeMap.put(lowerBoundInclusivity.equals("[") ? GREATER_THAN_OR_EQUAL_TO : GREATER_THAN, parsedLowerBound);
        }
        if(!upperBound.isEmpty()) {
            Object parsedUpperBound = numericTypeHandler.handle(columnType, columnName, upperBound);
            rangeMap.put(upperBoundInclusivity.equals("]") ? LESSER_THAN_OR_EQUAL_TO : LESSER_THAN, parsedUpperBound);
        }
        return rangeMap;
    }

    private Map<String, Object> handleTemporalRange(PostgresDataType columnType, String columnName, String lowerBoundInclusivity, String lowerBound, String upperBound, String upperBoundInclusivity) {
        Map<String, Object> rangeMap = new HashMap<>();

        if(!lowerBound.isEmpty()) {
            Object parsedLowerBound = temporalTypeHandler.handle(columnType, columnName, lowerBound);
            rangeMap.put(lowerBoundInclusivity.equals("[") ? GREATER_THAN_OR_EQUAL_TO : GREATER_THAN, parsedLowerBound);
        }

        if (!upperBound.isEmpty()) {
            Object parsedUpperBound = temporalTypeHandler.handle(columnType, columnName, upperBound);
            rangeMap.put(upperBoundInclusivity.equals("]") ? LESSER_THAN_OR_EQUAL_TO : LESSER_THAN, parsedUpperBound);
        }
        return rangeMap;
    }


    private String trimQuotes(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        String trimmed = input;

        if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

}
