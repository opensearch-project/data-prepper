/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.text.StringEscapeUtils;

/**
 * Credit to https://stackoverflow.com/questions/63655421/writing-parquet-avro-genericrecord-to-json-while-maintaining-logicaltypes
 */
public class GenericRecordJsonEncoder {

    Map<LogicalType, Function<Object, Object>> logicalTypesConverters = new HashMap<>();

    public void registerLogicalTypeConverter(LogicalType logicalType, Function<Object, Object> converter) {
        this.logicalTypesConverters.put(logicalType, converter);
    }

    public Function<Object, Object> getLogicalTypeConverter(Schema.Field field) {
        Schema fieldSchema = field.schema();
        LogicalType logicalType = fieldSchema.getLogicalType();
        return getLogicalTypeConverter(logicalType);
    }

    public Function<Object, Object> getLogicalTypeConverter(LogicalType logicalType) {
        if (logicalType == null) {
            return Function.identity();
        }

        return logicalTypesConverters.getOrDefault(logicalType, Function.identity());
    }

    public String serialize(GenericRecord value) {
        StringBuilder buffer = new StringBuilder();
        serialize(value, buffer, new IdentityHashMap<>(128) );
        String result = buffer.toString();
        return result;
    }

    private static final String TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT =
            " \">>> CIRCULAR REFERENCE CANNOT BE PUT IN JSON STRING, ABORTING RECURSION <<<\" ";

    /** Renders a Java datum as <a href="http://www.json.org/">JSON</a>. */
    private void serialize(final Object datum, final StringBuilder buffer, final IdentityHashMap<Object, Object> seenObjects) {
        if (isRecord(datum)) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            buffer.append("{");
            int count = 0;
            Schema schema = getRecordSchema(datum);
            for (Schema.Field f : schema.getFields()) {
                serialize(f.name(), buffer, seenObjects);
                buffer.append(": ");
                Function<Object, Object> logicalTypeConverter = getLogicalTypeConverter(f);
                serialize(logicalTypeConverter.apply(getField(datum, f.name(), f.pos())), buffer, seenObjects);
                if (++count < schema.getFields().size())
                    buffer.append(", ");
            }
            buffer.append("}");
            seenObjects.remove(datum);
        } else if (isArray(datum)) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            Collection<?> array = getArrayAsCollection(datum);
            buffer.append("[");
            long last = array.size()-1;
            int i = 0;
            for (Object element : array) {
                serialize(element, buffer, seenObjects);
                if (i++ < last)
                    buffer.append(", ");
            }
            buffer.append("]");
            seenObjects.remove(datum);
        } else if (isMap(datum)) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            buffer.append("{");
            int count = 0;
            @SuppressWarnings(value="unchecked")
            Map<Object,Object> map = (Map<Object,Object>)datum;
            for (Map.Entry<Object,Object> entry : map.entrySet()) {
                serialize(entry.getKey(), buffer, seenObjects);
                buffer.append(": ");
                serialize(entry.getValue(), buffer, seenObjects);
                if (++count < map.size())
                    buffer.append(", ");
            }
            buffer.append("}");
            seenObjects.remove(datum);
        } else if (isString(datum)|| isEnum(datum)) {
            buffer.append("\"");
            writeEscapedString(datum.toString(), buffer);
            buffer.append("\"");
        } else if (isBytes(datum)) {
            buffer.append("{\"bytes\": \"");
            ByteBuffer bytes = ((ByteBuffer) datum).duplicate();
            writeEscapedString(new String(bytes.array(), StandardCharsets.ISO_8859_1), buffer);
            buffer.append("\"}");
        } else if (((datum instanceof Float) &&       // quote Nan & Infinity
                (((Float)datum).isInfinite() || ((Float)datum).isNaN()))
                || ((datum instanceof Double) &&
                (((Double)datum).isInfinite() || ((Double)datum).isNaN()))) {
            buffer.append("\"");
            buffer.append(datum);
            buffer.append("\"");
        } else if (datum instanceof GenericData) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            serialize(datum, buffer, seenObjects);
            seenObjects.remove(datum);
        } else {
            // This fallback is the reason why GenericRecord toString does not
            // generate a valid JSON representation
            buffer.append(datum);
        }
    }

    // All these methods are also copied from the GenericData class source

    private boolean isRecord(Object datum) {
        return datum instanceof IndexedRecord;
    }

    private Schema getRecordSchema(Object record) {
        return ((GenericContainer)record).getSchema();
    }

    private Object getField(Object record, String name, int position) {
        return ((IndexedRecord)record).get(position);
    }

    private boolean isArray(Object datum) {
        return datum instanceof Collection;
    }

    private Collection getArrayAsCollection(Object datum) {
        return (Collection)datum;
    }

    private boolean isEnum(Object datum) {
        return datum instanceof GenericEnumSymbol;
    }

    private boolean isMap(Object datum) {
        return datum instanceof Map;
    }

    private boolean isString(Object datum) {
        return datum instanceof CharSequence;
    }

    private boolean isBytes(Object datum) {
        return datum instanceof ByteBuffer;
    }

    private void writeEscapedString(String string, StringBuilder builder) {
        builder.append(StringEscapeUtils.escapeJava(string));
    }
}