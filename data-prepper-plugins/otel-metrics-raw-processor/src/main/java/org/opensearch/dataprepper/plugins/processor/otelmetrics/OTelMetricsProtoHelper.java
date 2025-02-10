/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.otelmetrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.commons.codec.binary.Hex;
import org.opensearch.dataprepper.model.metric.Bucket;
import org.opensearch.dataprepper.model.metric.DefaultBucket;
import org.opensearch.dataprepper.model.metric.DefaultExemplar;
import org.opensearch.dataprepper.model.metric.DefaultQuantile;
import org.opensearch.dataprepper.model.metric.Exemplar;
import org.opensearch.dataprepper.model.metric.Quantile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class OTelMetricsProtoHelper {

    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsProtoHelper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String SERVICE_NAME = "service.name";
    private static final String METRIC_ATTRIBUTES = "metric.attributes";
    static final String RESOURCE_ATTRIBUTES = "resource.attributes";
    static final String EXEMPLAR_ATTRIBUTES = "exemplar.attributes";
    static final String INSTRUMENTATION_LIBRARY_NAME = "instrumentationLibrary.name";
    static final String INSTRUMENTATION_LIBRARY_VERSION = "instrumentationLibrary.version";
    static final String INSTRUMENTATION_SCOPE_NAME = "instrumentationScope.name";
    static final String INSTRUMENTATION_SCOPE_VERSION = "instrumentationScope.version";

    /**
     * To make it ES friendly we will replace '.' in keys with '@' in all the Keys in {@link io.opentelemetry.proto.common.v1.KeyValue}
     */
    private static final String DOT = ".";
    private static final String AT = "@";
    public static final Function<String, String> REPLACE_DOT_WITH_AT = i -> i.replace(DOT, AT);

    /**
     * Span and Resource attributes are essential for kibana so they should not be nested. SO we will prefix them with "metric.attributes"
     * and "resource.attributes" and "exemplar.attributes".
     */
    public static final Function<String, String> PREFIX_AND_METRIC_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> METRIC_ATTRIBUTES + DOT + i.replace(DOT, AT);
    public static final Function<String, String> PREFIX_AND_RESOURCE_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> RESOURCE_ATTRIBUTES + DOT + i.replace(DOT, AT);
    public static final Function<String, String> PREFIX_AND_EXEMPLAR_ATTRIBUTES_REPLACE_DOT_WITH_AT = i -> EXEMPLAR_ATTRIBUTES + DOT + i.replace(DOT, AT);

    private OTelMetricsProtoHelper() {
    }

    private static final Map<Integer, double[]> EXPONENTIAL_BUCKET_BOUNDS = new ConcurrentHashMap<>();

    /**
     * Converts an {@link AnyValue} into its appropriate data type
     *
     * @param value The value to convert
     * @return returns converted value object
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
     * @return A Map containing all attributes of `numberDataPoint` with keys converted into an OS-friendly format
     */
    public static Map<String, Object> convertKeysOfDataPointAttributes(final NumberDataPoint numberDataPoint) {
        return numberDataPoint.getAttributesList().stream()
                .collect(Collectors.toMap(i -> PREFIX_AND_METRIC_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    /**
     * Unpacks the List of {@link KeyValue} object into a Map.
     * Converts the keys into an os friendly format and casts the underlying data into its actual type?
     *
     * @param attributesList The list of {@link KeyValue} objects to process
     * @return A Map containing unpacked {@link KeyValue} data
     */
    public static Map<String, Object> unpackKeyValueList(List<KeyValue> attributesList) {
        return attributesList.stream()
                .collect(Collectors.toMap(i -> PREFIX_AND_METRIC_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }

    /**
     * Unpacks the List of {@link KeyValue} object into a Map.
     * Converts the keys into an os friendly format and casts the underlying data into its actual type?
     *
     * @param attributesList The list of {@link KeyValue} objects to process
     * @return A Map containing unpacked {@link KeyValue} data
     */
    public static Map<String, Object> unpackExemplarValueList(List<KeyValue> attributesList) {
        return attributesList.stream()
                .collect(Collectors.toMap(i -> PREFIX_AND_EXEMPLAR_ATTRIBUTES_REPLACE_DOT_WITH_AT.apply(i.getKey()), i -> convertAnyValue(i.getValue())));
    }


    /**
     * Extracts a value from the passed {@link NumberDataPoint} into a double representation
     *
     * @param ndp The {@link NumberDataPoint} which's data should be turned into a double value
     * @return A double representing the numerical value of the passed {@link NumberDataPoint}.
     * Null if the numerical data point is not present
     */
    public static Double getValueAsDouble(final NumberDataPoint ndp) {
        NumberDataPoint.ValueCase ndpCase = ndp.getValueCase();
        if (NumberDataPoint.ValueCase.AS_DOUBLE == ndpCase) {
            return ndp.getAsDouble();
        } else if (NumberDataPoint.ValueCase.AS_INT == ndpCase) {
            return (double) ndp.getAsInt();
        } else {
            return null;
        }
    }

    /**
     * Extracts a value from the passed {@link io.opentelemetry.proto.metrics.v1.Exemplar} into a double representation
     *
     * @param exemplar The {@link io.opentelemetry.proto.metrics.v1.Exemplar} which's data should be turned into a double value
     * @return A double representing the numerical value of the passed {@link io.opentelemetry.proto.metrics.v1.Exemplar}.
     * Null if the numerical data point is not present
     */
    public static Double getExemplarValueAsDouble(final io.opentelemetry.proto.metrics.v1.Exemplar exemplar) {
        io.opentelemetry.proto.metrics.v1.Exemplar.ValueCase valueCase = exemplar.getValueCase();
        if (io.opentelemetry.proto.metrics.v1.Exemplar.ValueCase.AS_DOUBLE == valueCase) {
            return exemplar.getAsDouble();
        } else if (io.opentelemetry.proto.metrics.v1.Exemplar.ValueCase.AS_INT == valueCase) {
            return (double) exemplar.getAsInt();
        } else {
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
     * @param instrumentationLibrary instrumentationLibrary
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

    /**
     * Extracts the name and version of the used instrumentation scope used
     *
     * @param instrumentationScope instrumentationScope
     * @return A map, containing information about the instrumentation scope
     */
    public static Map<String, Object> getInstrumentationScopeAttributes(final InstrumentationScope instrumentationScope) {
        final Map<String, Object> instrumentationScopeAttr = new HashMap<>();
        if (!instrumentationScope.getName().isEmpty()) {
            instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_NAME, instrumentationScope.getName());
        }
        if (!instrumentationScope.getVersion().isEmpty()) {
            instrumentationScopeAttr.put(INSTRUMENTATION_SCOPE_VERSION, instrumentationScope.getVersion());
        }
        return instrumentationScopeAttr;
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


    public static List<Quantile> getQuantileValues(List<SummaryDataPoint.ValueAtQuantile> quantileValues) {
        return quantileValues.stream()
                .map(q -> new DefaultQuantile(q.getQuantile(), q.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Create the buckets, see <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto">
     *     the OTel metrics proto spec</a>
     * <p> The boundaries for bucket at index i are: </p>
     * <pre>{@code
     * (-infinity, explicit_bounds[i]) for i == 0
     * (explicit_bounds[i-1], +infinity) for i == size(explicit_bounds)
     * (explicit_bounds[i-1], explicit_bounds[i]) for 0 < i < size(explicit_bounds)
     * }</pre>
     *
     * <br>
     * <br>
     * <b>NOTE:</b> here we map infinity as +/- FLOAT.MAX_VALUE since JSON rfc4627 only supports finite numbers and
     * OpenSearch maps double values to floats as per default.
     *
     * @param bucketCountsList   a list with the bucket counts
     * @param explicitBoundsList a list with the bounds
     * @return buckets list
     */
    public static List<Bucket> createBuckets(List<Long> bucketCountsList, List<Double> explicitBoundsList) {
        List<Bucket> buckets = new ArrayList<>();
        if (bucketCountsList.isEmpty()) {
            return buckets;
        }
        if (bucketCountsList.size() - 1 != explicitBoundsList.size()) {
            LOG.error("bucket count list not equals to bounds list {} {}", bucketCountsList.size(), explicitBoundsList.size());
            throw new IllegalArgumentException("OpenTelemetry protocol mandates that the number of elements in bucket_counts array must be by one greater than\n" +
                    "  // the number of elements in explicit_bounds array.");
        } else {
            for (int i = 0; i < bucketCountsList.size(); i++) {
                if (i == 0) {
                    double min = -Float.MAX_VALUE; // "-Infinity"
                    double max = explicitBoundsList.get(i);
                    Long bucketCount = bucketCountsList.get(i);
                    buckets.add(new DefaultBucket(min, max, bucketCount));
                } else if (i == bucketCountsList.size() - 1) {
                    double min = explicitBoundsList.get(i - 1);
                    double max = Float.MAX_VALUE; // "Infinity"
                    Long bucketCount = bucketCountsList.get(i);
                    buckets.add(new DefaultBucket(min, max, bucketCount));
                } else {
                    double min = explicitBoundsList.get(i - 1);
                    double max = explicitBoundsList.get(i);
                    Long bucketCount = bucketCountsList.get(i);
                    buckets.add(new DefaultBucket(min, max, bucketCount));
                }
            }
        }
        return buckets;
    }

    /**
     * Converts a List of {@link io.opentelemetry.proto.metrics.v1.Exemplar} values to {@link DefaultExemplar}, the
     * internal representation for Data Prepper
     *
     * @param exemplarsList the List of Exemplars
     * @return a mapped list of DefaultExemplars
     */
    public static List<Exemplar> convertExemplars(List<io.opentelemetry.proto.metrics.v1.Exemplar> exemplarsList) {
        return exemplarsList.stream().map(exemplar ->
                        new DefaultExemplar(convertUnixNanosToISO8601(exemplar.getTimeUnixNano()),
                                getExemplarValueAsDouble(exemplar),
                                Hex.encodeHexString(exemplar.getSpanId().toByteArray()),
                                Hex.encodeHexString(exemplar.getTraceId().toByteArray()),
                                unpackExemplarValueList(exemplar.getFilteredAttributesList())))
                .collect(Collectors.toList());
    }


    static double[] calculateBoundaries(int scale) {
        int len = 1 << Math.abs(scale);
        double[] boundaries = new double[len + 1];
        for (int i = 0; i <= len ; i++) {
            boundaries[i] = scale >=0 ?
                    Math.pow(2., i / (double) len) :
                    Math.pow(2., Math.pow(2., i));
        }
        return boundaries;
    }

    /**
     * Maps a List of {@link io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint.Buckets} to an
     * internal representation for Data Prepper.
     * See <a href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/datamodel.md#exponential-buckets">data model</a>
     *
     * @param buckets the list of buckets
     * @param scale the scale of the exponential histogram
     * @return a mapped list of Buckets
     */
    public static List<Bucket> createExponentialBuckets(ExponentialHistogramDataPoint.Buckets buckets, int scale) {
        double[] bucketBounds = EXPONENTIAL_BUCKET_BOUNDS.computeIfAbsent(scale, integer -> calculateBoundaries(scale));
        List<Bucket> mappedBuckets = new ArrayList<>();
        int offset = buckets.getOffset();
        List<Long> bucketsList = buckets.getBucketCountsList();
        for (int i = 0; i < bucketsList.size(); i++) {
            Long value = bucketsList.get(i);
            double lowerBound = bucketBounds[offset + i];
            double upperBound = bucketBounds[offset + i + 1];
            mappedBuckets.add(new DefaultBucket(lowerBound, upperBound, value));
        }
        return mappedBuckets;
    }
}
