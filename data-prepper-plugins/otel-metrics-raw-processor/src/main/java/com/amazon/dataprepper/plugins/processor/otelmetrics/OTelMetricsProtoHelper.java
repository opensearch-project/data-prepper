/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otelmetrics;

import com.amazon.dataprepper.model.metric.JacksonHistogram.Bucket;
import com.amazon.dataprepper.model.metric.JacksonSummary.SummaryQuantile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class OTelMetricsProtoHelper {

    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsProtoHelper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String SERVICE_NAME = "service.name";
    private static final String SPAN_ATTRIBUTES = "metric.attributes";
    static final String RESOURCE_ATTRIBUTES = "resource.attributes";
    static final String INSTRUMENTATION_LIBRARY_NAME = "instrumentationLibrary.name";
    static final String INSTRUMENTATION_LIBRARY_VERSION = "instrumentationLibrary.version";


    /**
     * To make it ES friendly we will replace '.' in keys with '@' in all the Keys in {@link io.opentelemetry.proto.common.v1.KeyValue}
     */
    private static final String DOT = ".";
    private static final String AT = "@";
    public static final Function<String, String> REPLACE_DOT_WITH_AT = i -> i.replace(DOT, AT);

    /**
     * Span and Resource attributes are essential for kibana so they should not be nested. SO we will prefix them with "span.attributes"
     * and "resource.attributes".
     */
    public static final Function<String, String> PREFIX_AND_SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> SPAN_ATTRIBUTES + DOT + i.replace(DOT, AT);
    public static final Function<String, String> PREFIX_AND_RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> RESOURCE_ATTRIBUTES + DOT + i.replace(DOT, AT);

    private OTelMetricsProtoHelper() {}


    /**
     * Converts an {@link AnyValue} into its appropriate data type
     *
     * @param value The value to convert
     *
     * @return
     */
    public static Object convertAnyValue(final AnyValue value) {
        switch (value.getValueCase()) {
            case VALUE_NOT_SET:
            case STRING_VALUE:
                return value.getStringValue();
            case BOOL_VALUE:
                return value.getBoolValue();
            case INT_VALUE:
                return value.getIntValue();
            case DOUBLE_VALUE:
                return value.getDoubleValue();
            /**
             * Both {@link AnyValue.ARRAY_VALUE_FIELD_NUMBER} and {@link AnyValue.KVLIST_VALUE_FIELD_NUMBER} are
             * nested objects. Storing them in flatten structure is not OpenSearch friendly. So they are stored
             * as Json string.
             */
            case ARRAY_VALUE:
                try {
                    return OBJECT_MAPPER.writeValueAsString(value.getArrayValue().getValuesList().stream()
                            .map(OTelMetricsProtoHelper::convertAnyValue)
                            .collect(Collectors.toList()));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            case KVLIST_VALUE:
                try {
                    return OBJECT_MAPPER.writeValueAsString(value.getKvlistValue().getValuesList().stream()
                            .collect(Collectors.toMap(i -> REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue()))));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            default:
                throw new RuntimeException(String.format("Can not convert AnyValue of type %s", value.getValueCase()));
        }
    }

    /**
     * Converts the keys of all attributes in the {@link NumberDataPoint}.
     * Also, casts the underlying data into its actual type
     *
     * @param numberDataPoint The point to process
     *
     * @return  A Map containing all attributes of `numberDataPoint` with keys converted into an OS-friendly format
     */
    public static Map<String, Object> convertKeysOfDataPointAttributes(final NumberDataPoint numberDataPoint) {
        return numberDataPoint.getAttributesList().stream()
                .collect(Collectors.toMap(i -> PREFIX_AND_SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    /**
     * Unpacks the List of {@link KeyValue} object into a Map.
     *
     * Converts the keys into an os friendly format and casts the underlying data into its actual type?
     *
     * @param attributesList The list of {@link KeyValue} objects to process
     *
     * @return  A Map containing unpacked {@link KeyValue} data
     */
    public static Map<String, Object> unpackKeyValueList(List<KeyValue> attributesList) {
        return attributesList.stream()
                .collect(Collectors.toMap(i -> PREFIX_AND_SPAN_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    /**
     * Extracts a value from the passed {@link NumberDataPoint} into a double representation
     *
     * @param ndp The {@link NumberDataPoint} which's data should be turned into a double value
     *
     * @return  A double representing the numerical value of the passed {@link NumberDataPoint}.
     * Null if the numerical data point is not present
     */
    public static Double getValueAsDouble(final NumberDataPoint ndp) {
        NumberDataPoint.ValueCase ndpCase = ndp.getValueCase();
        if (NumberDataPoint.ValueCase.AS_DOUBLE == ndpCase) {
            return ndp.getAsDouble();
        } else if (NumberDataPoint.ValueCase.AS_INT == ndpCase) {
            return (double) ndp.getAsInt();
        } else  {
            return null;
        }
    }

    public static Map<String, Object> getResourceAttributes(final Resource resource) {
        return resource.getAttributesList().stream()
                .collect(Collectors.toMap(i -> PREFIX_AND_RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    /**
     * Extracts the name and version of the used instrumentation library used
     *
     * @return A map, containing information about the instrumentation library
     */
    public static Map<String, Object> getInstrumentationLibraryAttributes(final InstrumentationLibrary instrumentationLibrary) {
        final Map<String, Object> instrumentationAttr = new HashMap<>();
        if (!instrumentationLibrary.getName().isEmpty()) {
            instrumentationAttr.put(INSTRUMENTATION_LIBRARY_NAME, instrumentationLibrary.getName());
        }
        if (!instrumentationLibrary.getVersion().isEmpty()) {
            instrumentationAttr.put(INSTRUMENTATION_LIBRARY_VERSION, instrumentationLibrary.getVersion());
        }
        return instrumentationAttr;
    }

    public static String convertUnixNanosToISO8601(final long unixNano) {
        return Instant.ofEpochSecond(0L, unixNano).toString();
    }

    public static String getStartTimeISO8601(final NumberDataPoint numberDataPoint) {
        return convertUnixNanosToISO8601(numberDataPoint.getStartTimeUnixNano());
    }

    public static String getTimeISO8601(final NumberDataPoint ndp) {
        return convertUnixNanosToISO8601(ndp.getTimeUnixNano());
    }

    public static Optional<String> getServiceName(final Resource resource) {
        return resource.getAttributesList().stream()
                .filter(keyValue -> keyValue.getKey().equals(SERVICE_NAME) && !keyValue.getValue().getStringValue().isEmpty())
                .findFirst()
                .map(i -> i.getValue().getStringValue());
    }


    public static Map<String, Object> mergeAllAttributes(final Collection<Map<String, Object>> attributes) {
        return attributes.stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }


    public static List<SummaryQuantile> getQuantileValues(List<SummaryDataPoint.ValueAtQuantile> quantileValues) {
        return quantileValues.stream()
                .map(q -> new SummaryQuantile(q.getQuantile(), q.getValue()))
                .collect(Collectors.toList());
    }

    public static List<Bucket> createBuckets(List<Long> bucketCountsList, List<Double> explicitBoundsList) {
        List<Bucket> buckets = new ArrayList<>();
        if (bucketCountsList.size() - 1 != explicitBoundsList.size()) {
            LOG.error("bucket count list not equals to bounds list {} {}", bucketCountsList.size(), explicitBoundsList.size() );
        } else {
            double previousBound = 0.0;
            for (int i = 0; i < bucketCountsList.size(); i++) {
                Long bucketCount = bucketCountsList.get(i);
                double bound = i == 0 ? previousBound : explicitBoundsList.get(i - 1);
                buckets.add(new Bucket(previousBound, bound, bucketCount));
                previousBound = bound;
            }
        }
        return buckets;
    }
}
