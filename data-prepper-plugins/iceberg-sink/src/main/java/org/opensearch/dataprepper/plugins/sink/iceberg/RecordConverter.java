/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Converts a Data Prepper event (Map) to an Iceberg {@link GenericRecord}
 * using the table schema, with type coercion for mismatched types.
 */
public class RecordConverter {

    private final Schema schema;

    public RecordConverter(final Schema schema) {
        this.schema = schema;
    }

    public GenericRecord convert(final Map<String, Object> data) {
        return convertStruct(data, schema.asStruct());
    }

    private GenericRecord convertStruct(final Map<String, Object> data, final Types.StructType structType) {
        final GenericRecord record = GenericRecord.create(structType);
        for (final Types.NestedField field : structType.fields()) {
            final Object value = data.get(field.name());
            if (value == null) {
                record.setField(field.name(), null);
            } else {
                record.setField(field.name(), convertValue(value, field.type()));
            }
        }
        return record;
    }

    @SuppressWarnings("unchecked")
    Object convertValue(final Object value, final Type type) {
        if (value == null) {
            return null;
        }
        switch (type.typeId()) {
            case BOOLEAN:
                return toBoolean(value);
            case INTEGER:
                return toInt(value);
            case LONG:
                return toLong(value);
            case FLOAT:
                return toFloat(value);
            case DOUBLE:
                return toDouble(value);
            case DECIMAL:
                return toDecimal(value, (Types.DecimalType) type);
            case STRING:
                return value.toString();
            case DATE:
                return toDate(value);
            case TIME:
                return toTime(value);
            case TIMESTAMP:
                return toTimestamp(value, (Types.TimestampType) type);
            case TIMESTAMP_NANO:
                return toTimestampNano(value, (Types.TimestampNanoType) type);
            case UUID:
                return UUID.fromString(value.toString());
            case BINARY:
            case FIXED:
            case GEOMETRY:
            case GEOGRAPHY:
                return toBinary(value);
            case STRUCT:
                return convertStruct((Map<String, Object>) value, type.asStructType());
            case LIST:
                return toList((List<?>) value, type.asListType());
            case MAP:
                return toMap((Map<?, ?>) value, type.asMapType());
            case VARIANT:
                return toVariant(value);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type.typeId());
        }
    }

    private boolean toBoolean(final Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private int toInt(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private long toLong(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
    }

    private float toFloat(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return Float.parseFloat(value.toString());
    }

    private double toDouble(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(value.toString());
    }

    private BigDecimal toDecimal(final Object value, final Types.DecimalType type) {
        final BigDecimal decimal;
        if (value instanceof BigDecimal) {
            decimal = (BigDecimal) value;
        } else if (value instanceof Number) {
            decimal = BigDecimal.valueOf(((Number) value).doubleValue());
        } else {
            decimal = new BigDecimal(value.toString());
        }
        return decimal.setScale(type.scale(), RoundingMode.HALF_UP);
    }

    private LocalDate toDate(final Object value) {
        if (value instanceof Number) {
            return LocalDate.ofEpochDay(((Number) value).longValue());
        }
        return LocalDate.parse(value.toString());
    }

    private LocalTime toTime(final Object value) {
        if (value instanceof Number) {
            return LocalTime.ofNanoOfDay(((Number) value).longValue() * 1_000_000L);
        }
        return LocalTime.parse(value.toString());
    }

    private static final DateTimeFormatter FLEXIBLE_TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .optionalStart().appendOffsetId().optionalEnd()
                    .toFormatter();

    private Object toTimestamp(final Object value, final Types.TimestampType type) {
        if (type.shouldAdjustToUTC()) {
            return toOffsetDateTime(value);
        }
        return toLocalDateTime(value);
    }

    private OffsetDateTime toOffsetDateTime(final Object value) {
        if (value instanceof Number) {
            return Instant.ofEpochMilli(((Number) value).longValue()).atOffset(ZoneOffset.UTC);
        }
        final TemporalAccessor parsed =
                FLEXIBLE_TIMESTAMP_FORMATTER.parseBest(value.toString(), OffsetDateTime::from, LocalDateTime::from);
        if (parsed instanceof OffsetDateTime) {
            return (OffsetDateTime) parsed;
        }
        return ((LocalDateTime) parsed).atOffset(ZoneOffset.UTC);
    }

    private LocalDateTime toLocalDateTime(final Object value) {
        if (value instanceof Number) {
            return LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(((Number) value).longValue()), ZoneOffset.UTC);
        }
        final TemporalAccessor parsed =
                FLEXIBLE_TIMESTAMP_FORMATTER.parseBest(value.toString(), OffsetDateTime::from, LocalDateTime::from);
        if (parsed instanceof LocalDateTime) {
            return (LocalDateTime) parsed;
        }
        return ((OffsetDateTime) parsed).toLocalDateTime();
    }

    private Object toTimestampNano(final Object value, final Types.TimestampNanoType type) {
        if (type.shouldAdjustToUTC()) {
            if (value instanceof Number) {
                final long nanos = ((Number) value).longValue();
                return Instant.ofEpochSecond(nanos / 1_000_000_000L, nanos % 1_000_000_000L)
                        .atOffset(ZoneOffset.UTC);
            }
            return toOffsetDateTime(value);
        }
        if (value instanceof Number) {
            final long nanos = ((Number) value).longValue();
            return LocalDateTime.ofInstant(
                    Instant.ofEpochSecond(nanos / 1_000_000_000L, nanos % 1_000_000_000L),
                    ZoneOffset.UTC);
        }
        return toLocalDateTime(value);
    }

    private ByteBuffer toBinary(final Object value) {
        if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        }
        return ByteBuffer.wrap(Base64.getDecoder().decode(value.toString()));
    }

    private List<?> toList(final List<?> value, final Types.ListType listType) {
        return value.stream()
                .map(element -> convertValue(element, listType.elementType()))
                .collect(Collectors.toList());
    }

    private Map<?, ?> toMap(final Map<?, ?> value, final Types.MapType mapType) {
        return value.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> convertValue(e.getKey(), mapType.keyType()),
                        e -> convertValue(e.getValue(), mapType.valueType())));
    }

    private Variant toVariant(final Object value) {
        final VariantMetadata metadata = buildVariantMetadata(value);
        return Variant.of(metadata, buildVariantValue(value, metadata));
    }

    private VariantMetadata buildVariantMetadata(final Object value) {
        final Set<String> fieldNames = new LinkedHashSet<>();
        collectFieldNames(value, fieldNames);
        return fieldNames.isEmpty() ? Variants.emptyMetadata() : Variants.metadata(fieldNames);
    }

    @SuppressWarnings("unchecked")
    private void collectFieldNames(final Object value, final Set<String> fieldNames) {
        if (value instanceof Map) {
            final Map<String, Object> map = (Map<String, Object>) value;
            fieldNames.addAll(map.keySet());
            map.values().forEach(v -> collectFieldNames(v, fieldNames));
        } else if (value instanceof List) {
            ((List<?>) value).forEach(v -> collectFieldNames(v, fieldNames));
        }
    }

    @SuppressWarnings("unchecked")
    private VariantValue buildVariantValue(
            final Object value, final VariantMetadata metadata) {
        if (value == null) {
            return Variants.ofNull();
        } else if (value instanceof Map) {
            final Map<String, Object> map = (Map<String, Object>) value;
            final ShreddedObject object = Variants.object(metadata);
            map.forEach((k, v) -> object.put(k, buildVariantValue(v, metadata)));
            return object;
        } else if (value instanceof List) {
            final ValueArray array = Variants.array();
            ((List<?>) value).forEach(v -> array.add(buildVariantValue(v, metadata)));
            return array;
        } else if (value instanceof Boolean) {
            return Variants.of((Boolean) value);
        } else if (value instanceof Integer) {
            return Variants.of((Integer) value);
        } else if (value instanceof Long) {
            return Variants.of((Long) value);
        } else if (value instanceof Float) {
            return Variants.of((Float) value);
        } else if (value instanceof Double) {
            return Variants.of((Double) value);
        } else if (value instanceof BigDecimal) {
            return Variants.of((BigDecimal) value);
        } else {
            return Variants.of(value.toString());
        }
    }
}
