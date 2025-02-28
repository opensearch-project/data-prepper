/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.client;

import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.or;
import static org.bson.BsonBinarySubType.UUID_STANDARD;

public class BsonHelper {
    static final String PARTITION_FORMAT = "%s-%s";
    private static final String PARTITION_SPLITTER = "-";
    private static final String NUMBER_TYPE = "number";
    public static final String MAX_KEY = "MaxKey";
    public static final String UNKNOWN_TYPE = "UNKNOWN";

    // https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
    /**
     * MongoDB uses the following comparison order for field types, from lowest to highest:
     * MinKey (internal type) // not used for _id field
     * Null // mongo will generate ObjectId for _id field
     * Numbers (ints, longs, doubles, decimals)
     * Symbol, String
     * Object
     * Array // not support for _id field
     * BinData
     * ObjectId
     * Boolean
     * Date
     * Timestamp
     * Regular Expression // not support for _id field
     * MaxKey (internal type) // not used for _id field
     */
    private static final List<String> RANGE_TYPE_ORDER = Arrays.asList(
        /*
        * These are internally represented as number.
        * java.lang.Integer
        * java.lang.Long
        * java.lang.Double
        * org.bson.types.Decimal128
        */
        NUMBER_TYPE,
        "java.lang.String",
        // "org.bson.types.Symbol", not supported in DocDB
        "org.bson.Document",
        "org.bson.types.Binary",
        "org.bson.types.ObjectId",
        "java.lang.Boolean",
        "java.util.Date",
        "org.bson.BsonDateTime",
        "org.bson.BsonTimestamp",
        MAX_KEY
        // "org.bson.types.Code" Javascript not supported in DocDB
        // "org.bson.BsonRegularExpression" Regex and Array not support as _id field
    );

    private static final Function<Object, Bson> GT_FUNCTION = a -> gt("_id", a);
    private static final Function<Object, Bson> GTE_FUNCTION = a -> gte("_id", a);
    private static final Function<Object, Bson> LTE_FUNCTION = a -> lte("_id", a);
    private static final String REGEX_PATTERN = "pattern";
    private static final String REGEX_OPTIONS = "options";
    public static final String DOCUMENTDB_ID_FIELD_NAME = "_id";

    public static final JsonWriterSettings JSON_WRITER_SETTINGS = JsonWriterSettings.builder()
        .outputMode(JsonMode.RELAXED)
        .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
        .binaryConverter((value, writer) ->  writer.writeString(getStringFromBsonBinary(value)))
        .dateTimeConverter((value, writer) -> writer.writeNumber(String.valueOf(value.longValue())))
        .decimal128Converter((value, writer) -> writer.writeString(value.bigDecimalValue().toPlainString()))
        .maxKeyConverter((value, writer) -> writer.writeNull())
        .minKeyConverter((value, writer) -> writer.writeNull())
        .regularExpressionConverter((value, writer) -> {
            writer.writeStartObject();
            writer.writeString(REGEX_PATTERN, value.getPattern());
            writer.writeString(REGEX_OPTIONS, value.getOptions());
            writer.writeEndObject();
        })
        .timestampConverter((value, writer) -> writer.writeNumber(String.valueOf(value.getTime())))
        .undefinedConverter((value, writer) -> writer.writeNull())
        .build();

    private static String getStringFromBsonBinary(final BsonBinary bsonBinary) {
        if (bsonBinary.getType() == UUID_STANDARD.getValue()) {
            return bsonBinary.asUuid().toString();
        } else {
            return Base64.getEncoder().encodeToString(bsonBinary.getData());
        }
    }

    public static String getPartitionStringFromMongoDBId(Object id, String className) {
        switch (className) {
            case "org.bson.Document":
                return ((Document) id).toJson(JSON_WRITER_SETTINGS);
            case "org.bson.types.Binary":
                final byte type = ((Binary) id).getType();
                final byte[] data = ((Binary) id).getData();
                final String typeString = String.valueOf(type);
                final String dataString = Base64.getEncoder().encodeToString(data);
                return String.format(PARTITION_FORMAT, typeString, dataString);
            case "java.util.Date":
                return String.valueOf(((Date) id).getTime());
            case "org.bson.BsonDateTime":
                return String.valueOf(((BsonDateTime) id).getValue());
            case "org.bson.BsonTimestamp":
                final int time = ((BsonTimestamp) id).getTime();
                final int inc = ((BsonTimestamp) id).getInc();
                return String.format(PARTITION_FORMAT, time, inc);
            case "org.bson.types.Code":
                return ((Code) id).getCode();
            case "org.bson.types.Decimal128":
                return ((Decimal128) id).bigDecimalValue().toPlainString();
            default:
                return id.toString();
        }
    }

    private static Bson buildAndQuery(final String gte, final String lte, final String gteClassName, final String lteClassName) {
        return and(
            buildQuery(GTE_FUNCTION, gte, gteClassName),
            buildQuery(LTE_FUNCTION, lte, lteClassName)
        );
    }

    private static Bson buildQuery(final Function<Object, Bson> function, final String value, final String className) {
        switch (className) {
            case "java.lang.Integer":
                return function.apply(Integer.parseInt(value));
            case "java.lang.Long":
                return function.apply(Long.parseLong(value));
            case "java.lang.Double":
                return function.apply(Double.parseDouble(value));
            case "org.bson.types.Decimal128":
                return function.apply(Decimal128.parse(value));
            case "java.lang.String":
                return function.apply(value);
            case "org.bson.types.Symbol":
                return function.apply(new Symbol(value));
            case "org.bson.Document":
                return function.apply(Document.parse(value));
            case "org.bson.types.Binary":
                String[] binaryString = value.split(PARTITION_SPLITTER, 2);
                return function.apply(new Binary(Byte.parseByte(binaryString[0]), Base64.getDecoder()
                        .decode(binaryString[1])));
            case "org.bson.types.ObjectId":
                return function.apply(new ObjectId(value));
            case "java.lang.Boolean":
                return function.apply(Boolean.parseBoolean(value));
            case "java.util.Date":
            case "org.bson.BsonDateTime":
                return function.apply(new BsonDateTime(Long.parseLong(value)));
            case "org.bson.BsonTimestamp":
                String[] timestampString = value.split(PARTITION_SPLITTER, 2);
                return function.apply(new BsonTimestamp(Integer.parseInt(timestampString[0]), Integer.parseInt(timestampString[1])));
            case "org.bson.types.Code":
                return function.apply(new Code(value));
            default:
                throw new RuntimeException("Unexpected _id class not supported: " + className);
        }
    }

    private static boolean isClassNumber(final String className) {
        return className.equals("java.lang.Integer") || className.equals("java.lang.Long") || className.equals("java.lang.Double")
                || className.equals("org.bson.types.Decimal128");
    }

    public static Bson buildGtQuery(final String greaterThan, final String gtClassName, final String lteClassName) {
        Bson bsonQuery = buildQuery(GT_FUNCTION, greaterThan, gtClassName);
        return buildSortOrderQuery(bsonQuery, gtClassName, lteClassName);
    }

    private static Bson buildGteQuery(final String greaterThanEquals, final String gteClassName, final String lteClassName) {
        Bson bsonQuery = buildQuery(GTE_FUNCTION, greaterThanEquals, gteClassName);
        return buildSortOrderQuery(bsonQuery, gteClassName, lteClassName);
    }

    private static Bson buildSortOrderQuery(Bson bsonQuery, final String gtClassName, final String lteClassName) {
        int prev_i;
        if (isClassNumber(gtClassName)) {
            prev_i = RANGE_TYPE_ORDER.indexOf(NUMBER_TYPE);
        } else {
            prev_i = RANGE_TYPE_ORDER.indexOf(gtClassName);
        }
        int curr_i;
        if (isClassNumber(lteClassName)) {
            curr_i = RANGE_TYPE_ORDER.indexOf(NUMBER_TYPE);
        } else {
            curr_i = RANGE_TYPE_ORDER.indexOf(lteClassName);
        }
        for (int i = prev_i + 1; i < curr_i; i++) {
            String className = RANGE_TYPE_ORDER.get(i);
            switch (className) {
                case "java.lang.Integer":
                case "java.lang.Long":
                case "java.lang.Double":
                case "org.bson.types.Decimal128":
                    bsonQuery = or(
                        bsonQuery,
                        gte("_id", new BsonInt32(0))
                    );
                    break;
                case "java.lang.String":
                    bsonQuery = or(
                        bsonQuery,
                        gte("_id", new BsonString(""))
                    );
                    break;
                case "org.bson.types.Symbol":
                    bsonQuery = or(
                        bsonQuery,
                        gte("_id", new Symbol(""))
                    );
                    break;
                case "org.bson.Document":
                    bsonQuery = or(
                        bsonQuery,
                        gte("_id", new Document())
                    );
                    break;
                case "org.bson.types.ObjectId":
                    bsonQuery = or(
                        bsonQuery,
                        gte("_id", new BsonObjectId(new ObjectId("000000000000000000000000")))
                    );
                    break;
                case "java.lang.Boolean":
                    bsonQuery = or(
                        bsonQuery,
                        in("_id", true, false)
                    );
                    break;
                case "org.bson.types.Binary":
                    bsonQuery = or(
                        bsonQuery,
                        gte("_id", new BsonBinary(new byte[0]))
                    );
                    break;
                case "java.util.Date":
                case "org.bson.BsonDateTime":
                    bsonQuery = or(
                        bsonQuery,
                        gte("_id", new BsonDateTime(0))
                    );
                    break;
                case "org.bson.BsonTimestamp":
                    bsonQuery = or(
                        bsonQuery,
                        gte("_id", new BsonTimestamp(0))
                    );
                    break;
                case "org.bson.types.Code":
                    bsonQuery = or(
                        bsonQuery,
                        gte("_id", new Code(""))
                    );
                    break;
                case MAX_KEY:
                    // do nothing. This is used internally for returning all values GT than a key and less than this
                    // internal key
                    break;
                default:
                    throw new RuntimeException("Unexpected _id class not supported: " + className);
            }
        }

        return bsonQuery;
    }

    public static Bson buildQuery(final String gte, final String lte, final String gteClassName, final String lteClassName) {
        if (gteClassName.equals(lteClassName) || (isClassNumber(gteClassName) && isClassNumber(lteClassName))) {
            return buildAndQuery(gte, lte, gteClassName, lteClassName);
        }

        final Bson bsonQuery = buildGteQuery(gte, gteClassName, lteClassName);

        return or(
            bsonQuery,
            buildQuery(LTE_FUNCTION, lte, lteClassName)
        );
    }
}
