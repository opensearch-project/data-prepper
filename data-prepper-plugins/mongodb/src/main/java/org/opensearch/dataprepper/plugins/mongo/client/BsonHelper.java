/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.client;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;

public class BsonHelper {
    private static final String BINARY_PARTITION_FORMAT = "%s-%s";
    private static final String BINARY_PARTITION_SPLITTER = "-";
    private static final String TIMESTAMP_PARTITION_FORMAT = "%s-%s";
    private static final String TIMESTAMP_PARTITION_SPLITTER = "-";

    public static String getPartitionStringFromMongoDBId(Object id, String className) {
        switch (className) {
            case "org.bson.Document":
                return ((Document) id).toJson();
            case "org.bson.types.Binary":
                final byte type = ((Binary) id).getType();
                final byte[] data = ((Binary) id).getData();
                String typeString = String.valueOf(type);
                String dataString = new String(data);
                return String.format(BINARY_PARTITION_FORMAT, typeString, dataString);
            case "org.bson.types.BSONTimestamp":
                final int inc = ((BSONTimestamp) id).getInc();
                final int time = ((BSONTimestamp) id).getTime();
                return String.format(TIMESTAMP_PARTITION_FORMAT, inc, time);
            case "org.bson.types.Code":
                return ((Code) id).getCode();
            default:
                return id.toString();
        }
    }

    public static Bson buildAndQuery(String gte, String lte, String className) {
        switch (className) {
            case "java.lang.Integer":
                return and(
                        gte("_id", Integer.parseInt(gte)),
                        lte("_id", Integer.parseInt(lte))
                );
            case "java.lang.Double":
                return and(
                        gte("_id", Double.parseDouble(gte)),
                        lte("_id", Double.parseDouble(lte))
                );
            case "java.lang.String":
                return and(
                        gte("_id", gte),
                        lte("_id", lte)
                );
            case "java.lang.Long":
                return and(
                        gte("_id", Long.parseLong(gte)),
                        lte("_id", Long.parseLong(lte))
                );
            case "org.bson.types.ObjectId":
                return and(
                        gte("_id", new ObjectId(gte)),
                        lte("_id", new ObjectId(lte))
                );
            case "org.bson.types.Decimal128":
                return and(
                        gte("_id", Decimal128.parse(gte)),
                        lte("_id", Decimal128.parse(lte))
                );
            case "org.bson.types.Binary":
                String[] gteString = gte.split(BINARY_PARTITION_SPLITTER, 2);
                String[] lteString = lte.split(BINARY_PARTITION_SPLITTER, 2);
                return and(
                        gte("_id", new Binary(Byte.parseByte(gteString[0]), gteString[1].getBytes())),
                        lte("_id", new Binary(Byte.parseByte(lteString[0]), lteString[1].getBytes()))
                );
            case "org.bson.types.BSONTimestamp":
                String[] gteTimestampString = gte.split(TIMESTAMP_PARTITION_SPLITTER, 2);
                String[] lteTimestampString = lte.split(TIMESTAMP_PARTITION_SPLITTER, 2);
                return and(
                        gte("_id", new BSONTimestamp(Integer.parseInt(gteTimestampString[0]), Integer.parseInt(gteTimestampString[1]))),
                        lte("_id", new BSONTimestamp(Integer.parseInt(lteTimestampString[0]), Integer.parseInt(lteTimestampString[1])))
                );
            case "org.bson.types.Code":
                return and(
                        gte("_id", new Code(gte)),
                        lte("_id", new Code(lte))
                );
            case "org.bson.types.Symbol":
                return and(
                        gte("_id", new Symbol(gte)),
                        lte("_id", new Symbol(lte))
                );
            case "org.bson.Document":
                return and(
                        gte("_id", Document.parse(gte)),
                        lte("_id", Document.parse(lte))
                );
            default:
                throw new RuntimeException("Unexpected _id class not supported: " + className);
        }
    }
}
