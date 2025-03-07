package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.utils.PgArrayParser;
import org.postgresql.util.PGmoney;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class NumericTypeHandler implements PostgresDataTypeHandler {

    private static final String NAN = "nan";
    private static final String INFINITY = "infinity";
    private static final String NEGATIVE_INFINITY = "-infinity";

    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isNumeric()) {
            throw new IllegalArgumentException("ColumnType is not numeric: " + columnType);
        }
        if (columnType.isSubCategoryArray())
            return PgArrayParser.parseTypedArray(value.toString(), PostgresDataType.getScalarType(columnType),
                    this::parseNumericValue);
        return parseNumericValue(columnType, value.toString());
    }

    private Object parseNumericValue(PostgresDataType columnType, String textValue) {
        switch (columnType) {
            case SMALLINT:
            case SMALLSERIAL:
                return Short.parseShort(textValue);
            case INTEGER:
            case SERIAL:
                return Integer.parseInt(textValue);
            case BIGINT:
            case BIGSERIAL:
                return Long.parseLong(textValue);
            case REAL:
                return parseRealValue(textValue);
            case DOUBLE_PRECISION:
                return parseDoublePrecisionValue(textValue);
            case NUMERIC:
                return textValue;
            case MONEY:
                return parseMoney(textValue);
            default:
                return textValue;
        }
    }

    private Object parseRealValue(String textValue) {
        switch (textValue.toLowerCase()) {
            case NAN:
                return Float.NaN;
            case INFINITY:
                return Float.POSITIVE_INFINITY;
            case NEGATIVE_INFINITY:
                return Float.NEGATIVE_INFINITY;
            default:
                return Float.parseFloat(textValue);
        }
    }

    private Object parseDoublePrecisionValue(String textValue) {
        switch (textValue.toLowerCase()) {
            case NAN:
                return Double.NaN;
            case INFINITY:
                return Double.POSITIVE_INFINITY;
            case NEGATIVE_INFINITY:
                return Double.NEGATIVE_INFINITY;
            default:
                return Double.parseDouble(textValue);
        }
    }

    private Object parseMoney(String textValue) {
        try {
            boolean isNegative = textValue.charAt(0) == '-';

            if (isNegative) {
                textValue = textValue.substring(1);
            }
            char currencySymbol = textValue.charAt(0);
            PGmoney money = new PGmoney(textValue);
            double value = isNegative ? -money.val : money.val;
            Map<String, Object> moneyMap = new HashMap<>();
            moneyMap.put("currency", currencySymbol);
            moneyMap.put("amount", value);
            return moneyMap;
        } catch (SQLException e) {
            throw new RuntimeException("Error parsing String to PGmoney object", e);
        }
    }

}
