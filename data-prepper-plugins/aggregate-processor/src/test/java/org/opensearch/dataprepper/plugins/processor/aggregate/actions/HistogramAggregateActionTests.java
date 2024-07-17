/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.metric.Exemplar;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionTestUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

@ExtendWith(MockitoExtension.class)
public class HistogramAggregateActionTests {
    private AggregateAction histogramAggregateAction;
    private HistogramAggregateActionConfig histogramAggregateActionConfig;

    @BeforeEach
    void setUp() {
        histogramAggregateActionConfig = new HistogramAggregateActionConfig();
    }

    private AggregateAction createObjectUnderTest() {
        return new HistogramAggregateAction(histogramAggregateActionConfig);
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 20, 50, 100})
    void testHistogramAggregate(final int testCount) throws NoSuchFieldException, IllegalAccessException {
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "outputFormat", OutputFormat.RAW.toString());
        final String testKeyPrefix = RandomStringUtils.randomAlphabetic(5)+"_";
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "generatedKeyPrefix", testKeyPrefix);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "units", "ms");
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "recordMinMax", true);
        final double TEST_VALUE_RANGE_MIN = 0.0;
        final double TEST_VALUE_RANGE_MAX = 6.0;
        final double TEST_VALUE_RANGE_STEP = 2.0;
        final double bucket1 = TEST_VALUE_RANGE_MIN;
        final double bucket2 = bucket1 + TEST_VALUE_RANGE_STEP;
        final double bucket3 = bucket2 + TEST_VALUE_RANGE_STEP;
        List<Double> buckets = new ArrayList<Double>();
        buckets.add(bucket1);
        buckets.add(bucket2);
        buckets.add(bucket3);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", buckets);
        final String testKey = RandomStringUtils.randomAlphabetic(10);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "key", testKey);
        histogramAggregateAction = createObjectUnderTest();
        final String dataKey = RandomStringUtils.randomAlphabetic(10);
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        Long[] expectedBucketCounts = new Long[buckets.size()+1];
        double expectedSum = 0.0;
        double expectedMin = TEST_VALUE_RANGE_MAX+TEST_VALUE_RANGE_STEP+1.0;
        double expectedMax = TEST_VALUE_RANGE_MIN-TEST_VALUE_RANGE_STEP-1.0;
        Arrays.fill(expectedBucketCounts, (long)0);
        for (int i = 0; i < testCount; i++) { 
            final double value = ThreadLocalRandom.current().nextDouble(TEST_VALUE_RANGE_MIN-TEST_VALUE_RANGE_STEP, TEST_VALUE_RANGE_MAX+TEST_VALUE_RANGE_STEP);
            if (value < bucket1) {
                expectedBucketCounts[0]++;
            } else if (value < bucket2) {
                expectedBucketCounts[1]++;
            } else if (value < bucket3) {
                expectedBucketCounts[2]++;
            } else {
                expectedBucketCounts[3]++;
            }
            expectedSum += value;
            if (value < expectedMin) {
                expectedMin = value;
            }
            if (value > expectedMax) {
                expectedMax = value;
            }
            Map<Object, Object> eventMap = Collections.singletonMap(testKey, value);
            Event testEvent = JacksonEvent.builder()
                    .withEventType("event")
                    .withData(eventMap)
                    .build();
            testEvent.put(dataKey, RandomStringUtils.randomAlphabetic(15));
            final AggregateActionResponse aggregateActionResponse = histogramAggregateAction.handleEvent(testEvent, aggregateActionInput);
            assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        }

        final AggregateActionOutput actionOutput = histogramAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertThat(result.size(), equalTo(1));
        final String expectedCountKey = histogramAggregateActionConfig.getCountKey();
        final String expectedStartTimeKey = histogramAggregateActionConfig.getStartTimeKey();
        Map<String, Object> expectedEventMap = new HashMap<>(Collections.singletonMap(expectedCountKey, testCount));
        
        final String expectedSumKey = histogramAggregateActionConfig.getSumKey();
        expectedEventMap.put(expectedSumKey, expectedSum);
        final String expectedMaxKey = histogramAggregateActionConfig.getMaxKey();
        expectedEventMap.put(expectedMaxKey, expectedMax);
        final String expectedMinKey = histogramAggregateActionConfig.getMinKey();
        expectedEventMap.put(expectedMinKey, expectedMin);
        expectedEventMap.forEach((k, v) -> assertThat(result.get(0).toMap(), hasEntry(k,v)));
        assertThat(result.get(0).toMap(), hasKey(expectedStartTimeKey));
        final String expectedDurationKey = histogramAggregateActionConfig.getDurationKey();
        assertThat(result.get(0).toMap(), hasKey(expectedDurationKey));
        final String expectedBucketCountsKey = histogramAggregateActionConfig.getBucketCountsKey();
        final List<Long> bucketCountsFromResult = (ArrayList<Long>)result.get(0).toMap().get(expectedBucketCountsKey);
        for (int i = 0; i < expectedBucketCounts.length; i++) {
            assertThat(expectedBucketCounts[i], equalTo(bucketCountsFromResult.get(i)));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 20, 50, 100})
    void testHistogramAggregateOTelFormat(final int testCount) throws NoSuchFieldException, IllegalAccessException {
        final String testKeyPrefix = RandomStringUtils.randomAlphabetic(5)+"_";
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "generatedKeyPrefix", testKeyPrefix);
        final String testUnits = "ms";
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "units", testUnits);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "recordMinMax", true);
        final double TEST_VALUE_RANGE_MIN = 0.0;
        final double TEST_VALUE_RANGE_MAX = 6.0;
        final double TEST_VALUE_RANGE_STEP = 2.0;
        final double bucket1 = TEST_VALUE_RANGE_MIN;
        final double bucket2 = bucket1 + TEST_VALUE_RANGE_STEP;
        final double bucket3 = bucket2 + TEST_VALUE_RANGE_STEP;
        List<Double> buckets = new ArrayList<Double>();
        buckets.add(bucket1);
        buckets.add(bucket2);
        buckets.add(bucket3);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", buckets);
        final String testKey = RandomStringUtils.randomAlphabetic(10);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "key", testKey);
        histogramAggregateAction = createObjectUnderTest();
        final String dataKey = RandomStringUtils.randomAlphabetic(10);
        final String dataValue = RandomStringUtils.randomAlphabetic(15);
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Map.of(dataKey, dataValue));
        Long[] expectedBucketCounts = new Long[buckets.size()+1];
        double expectedSum = 0.0;
        double expectedMin = TEST_VALUE_RANGE_MAX+TEST_VALUE_RANGE_STEP+1.0;
        double expectedMax = TEST_VALUE_RANGE_MIN-TEST_VALUE_RANGE_STEP-1.0;
        Arrays.fill(expectedBucketCounts, (long)0);
        for (int i = 0; i < testCount; i++) { 
            final double value = ThreadLocalRandom.current().nextDouble(TEST_VALUE_RANGE_MIN-TEST_VALUE_RANGE_STEP, TEST_VALUE_RANGE_MAX+TEST_VALUE_RANGE_STEP);
            if (value < bucket1) {
                expectedBucketCounts[0]++;
            } else if (value < bucket2) {
                expectedBucketCounts[1]++;
            } else if (value < bucket3) {
                expectedBucketCounts[2]++;
            } else {
                expectedBucketCounts[3]++;
            }
            expectedSum += value;
            if (value < expectedMin) {
                expectedMin = value;
            }
            if (value > expectedMax) {
                expectedMax = value;
            }
            Map<Object, Object> eventMap = Collections.singletonMap(testKey, value);
            Event testEvent = JacksonEvent.builder()
                    .withEventType("event")
                    .withData(eventMap)
                    .build();
            final AggregateActionResponse aggregateActionResponse = histogramAggregateAction.handleEvent(testEvent, aggregateActionInput);
            assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        }

        final AggregateActionOutput actionOutput = histogramAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertThat(result.size(), equalTo(1));
        Map<String, Object> expectedEventMap = new HashMap<>(Collections.singletonMap("count", (long)testCount));
        expectedEventMap.put("unit", testUnits);
        expectedEventMap.put("name", HistogramAggregateActionConfig.HISTOGRAM_METRIC_NAME);
        expectedEventMap.put("sum", expectedSum);
        expectedEventMap.put("min", expectedMin);
        expectedEventMap.put("max", expectedMax);
        expectedEventMap.put("bucketCounts", expectedBucketCounts.length);
        expectedEventMap.put("explicitBoundsCount", expectedBucketCounts.length-1);
        
        expectedEventMap.forEach((k, v) -> assertThat(result.get(0).toMap(), hasEntry(k, v)));
        assertThat(result.get(0).toMap(), hasKey("startTime"));
        assertThat(result.get(0).toMap(), hasKey("time"));
        final List<Long> bucketCountsFromResult = (ArrayList<Long>)result.get(0).toMap().get("bucketCountsList");
        for (int i = 0; i < expectedBucketCounts.length; i++) {
            assertThat(expectedBucketCounts[i], equalTo(bucketCountsFromResult.get(i)));
        }
        assertThat(((Map<String, String>)result.get(0).toMap().get("attributes")), hasEntry(HistogramAggregateActionConfig.HISTOGRAM_METRIC_NAME+"_key", testKey));
        List<Exemplar> exemplars = (List <Exemplar>)result.get(0).toMap().get("exemplars");
        assertThat(exemplars.size(), equalTo(2));
        assertThat(((Map<String, String>)result.get(0).toMap().get("attributes")), hasEntry(dataKey, dataValue));
        final String expectedDurationKey = histogramAggregateActionConfig.getDurationKey();
        assertThat(((Map<String, String>)result.get(0).toMap().get("attributes")), hasKey(expectedDurationKey));
        JacksonMetric metric = (JacksonMetric) result.get(0);
        assertThat(metric.toJsonString().indexOf("attributes"), not(-1));
        final List<Double> explicitBoundsFromResult = (ArrayList<Double>)result.get(0).toMap().get("explicitBounds");
        double bucketVal = TEST_VALUE_RANGE_MIN;
        for (int i = 0; i < explicitBoundsFromResult.size(); i++) {
            assertThat(explicitBoundsFromResult.get(i), equalTo(bucketVal));
            bucketVal += TEST_VALUE_RANGE_STEP;
        }
        final List<Map<String, Object>> bucketsFromResult = (ArrayList<Map<String, Object>>)result.get(0).toMap().get("buckets");
        double expectedBucketMin = -Float.MAX_VALUE;
        double expectedBucketMax = TEST_VALUE_RANGE_MIN;
        for (int i = 0; i < bucketsFromResult.size(); i++) {
            assertThat(bucketsFromResult.get(i), hasEntry("min", expectedBucketMin));
            assertThat(bucketsFromResult.get(i), hasEntry("max", expectedBucketMax));
            assertThat(bucketsFromResult.get(i), hasEntry("count", expectedBucketCounts[i]));
            expectedBucketMin = expectedBucketMax;
            expectedBucketMax += TEST_VALUE_RANGE_STEP;
            if (i == bucketsFromResult.size()-2) {
                expectedBucketMax = Float.MAX_VALUE;
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 20, 50, 100})
    void testHistogramAggregateOTelFormatWithStartAndEndTimesInTheEvent(final int testCount) {
        histogramAggregateActionConfig = mock(HistogramAggregateActionConfig.class);
        String startTimeKey = UUID.randomUUID().toString();
        String endTimeKey = UUID.randomUUID().toString();
        final String testKeyPrefix = RandomStringUtils.randomAlphabetic(5)+"_";
        when(histogramAggregateActionConfig.getStartTimeKey()).thenReturn(startTimeKey);
        when(histogramAggregateActionConfig.getEndTimeKey()).thenReturn(endTimeKey);
        final String testName = UUID.randomUUID().toString();
        when(histogramAggregateActionConfig.getMetricName()).thenReturn(testName);
        when(histogramAggregateActionConfig.getOutputFormat()).thenReturn(OutputFormat.OTEL_METRICS.toString());
        String keyPrefix = UUID.randomUUID().toString();
        final String testUnits = "ms";
        when(histogramAggregateActionConfig.getUnits()).thenReturn(testUnits);
        when(histogramAggregateActionConfig.getRecordMinMax()).thenReturn(true);
        final double TEST_VALUE_RANGE_MIN = 0.0;
        final double TEST_VALUE_RANGE_MAX = 6.0;
        final double TEST_VALUE_RANGE_STEP = 2.0;
        final double bucket1 = TEST_VALUE_RANGE_MIN;
        final double bucket2 = bucket1 + TEST_VALUE_RANGE_STEP;
        final double bucket3 = bucket2 + TEST_VALUE_RANGE_STEP;
        List<Number> buckets = new ArrayList<Number>();
        buckets.add(bucket1);
        buckets.add(bucket2);
        buckets.add(bucket3);
        when(histogramAggregateActionConfig.getBuckets()).thenReturn(buckets);
        final String testKey = RandomStringUtils.randomAlphabetic(10);
        when(histogramAggregateActionConfig.getKey()).thenReturn(testKey);
        final String testPrefix = RandomStringUtils.randomAlphabetic(7);
        when(histogramAggregateActionConfig.getSumKey()).thenReturn(testPrefix+"sum");
        when(histogramAggregateActionConfig.getMinKey()).thenReturn(testPrefix+"min");
        when(histogramAggregateActionConfig.getMaxKey()).thenReturn(testPrefix+"max");
        when(histogramAggregateActionConfig.getCountKey()).thenReturn(testPrefix+"count");
        when(histogramAggregateActionConfig.getBucketsKey()).thenReturn(testPrefix+"buckets");
        when(histogramAggregateActionConfig.getBucketCountsKey()).thenReturn(testPrefix+"bucketcounts");
        when(histogramAggregateActionConfig.getDurationKey()).thenReturn(testPrefix+"duration");
        histogramAggregateAction = createObjectUnderTest();
        final String dataKey = RandomStringUtils.randomAlphabetic(10);
        final String dataValue = RandomStringUtils.randomAlphabetic(15);
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Map.of(dataKey, dataValue));
        Long[] expectedBucketCounts = new Long[buckets.size()+1];
        double expectedSum = 0.0;
        double expectedMin = TEST_VALUE_RANGE_MAX+TEST_VALUE_RANGE_STEP+1.0;
        double expectedMax = TEST_VALUE_RANGE_MIN-TEST_VALUE_RANGE_STEP-1.0;
        Arrays.fill(expectedBucketCounts, (long)0);
        Random random = new Random();
        final Instant testTime = Instant.ofEpochSecond(Instant.now().getEpochSecond());
        for (int i = 0; i < testCount; i++) {
            final double value = ThreadLocalRandom.current().nextDouble(TEST_VALUE_RANGE_MIN-TEST_VALUE_RANGE_STEP, TEST_VALUE_RANGE_MAX+TEST_VALUE_RANGE_STEP);
            if (value < bucket1) {
                expectedBucketCounts[0]++;
            } else if (value < bucket2) {
                expectedBucketCounts[1]++;
            } else if (value < bucket3) {
                expectedBucketCounts[2]++;
            } else {
                expectedBucketCounts[3]++;
            }
            expectedSum += value;
            if (value < expectedMin) {
                expectedMin = value;
            }
            if (value > expectedMax) {
                expectedMax = value;
            }
            Instant sTime = (i == 0) ? testTime : testTime.plusSeconds(random.nextInt(5));
            Instant eTime = (i == testCount-1) ? testTime.plusSeconds(100) : testTime.plusSeconds (50+random.nextInt(45));
            Map<Object, Object> eventMap = Collections.synchronizedMap(Map.of(testKey, value, startTimeKey, sTime, endTimeKey, eTime));
            Event testEvent = JacksonEvent.builder()
                    .withEventType("event")
                    .withData(eventMap)
                    .build();
            final AggregateActionResponse aggregateActionResponse = histogramAggregateAction.handleEvent(testEvent, aggregateActionInput);
            assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        }

        final AggregateActionOutput actionOutput = histogramAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertThat(result.size(), equalTo(1));
        Map<String, Object> expectedEventMap = new HashMap<>(Collections.singletonMap("count", (long)testCount));
        expectedEventMap.put("unit", testUnits);
        expectedEventMap.put("name", testName);
        expectedEventMap.put("sum", expectedSum);
        expectedEventMap.put("min", expectedMin);
        expectedEventMap.put("max", expectedMax);
        expectedEventMap.put("bucketCounts", expectedBucketCounts.length);
        expectedEventMap.put("explicitBoundsCount", expectedBucketCounts.length-1);

        expectedEventMap.forEach((k, v) -> assertThat(result.get(0).toMap(), hasEntry(k, v)));
        assertThat(result.get(0).toMap(), hasKey("startTime"));
        assertThat(result.get(0).toMap(), hasKey("time"));
        final List<Long> bucketCountsFromResult = (ArrayList<Long>)result.get(0).toMap().get("bucketCountsList");
        for (int i = 0; i < expectedBucketCounts.length; i++) {
            assertThat(expectedBucketCounts[i], equalTo(bucketCountsFromResult.get(i)));
        }
        assertThat(((Map<String, String>)result.get(0).toMap().get("attributes")), hasEntry(testName+"_key", testKey));
        List<Exemplar> exemplars = (List <Exemplar>)result.get(0).toMap().get("exemplars");
        assertThat(exemplars.size(), equalTo(2));
        assertThat(((Map<String, String>)result.get(0).toMap().get("attributes")), hasEntry(dataKey, dataValue));
        final String expectedDurationKey = histogramAggregateActionConfig.getDurationKey();
        assertThat(((Map<String, String>)result.get(0).toMap().get("attributes")), hasKey(expectedDurationKey));
        JacksonMetric metric = (JacksonMetric) result.get(0);
        assertThat(metric.toJsonString().indexOf("attributes"), not(-1));
        final List<Double> explicitBoundsFromResult = (ArrayList<Double>)result.get(0).toMap().get("explicitBounds");
        double bucketVal = TEST_VALUE_RANGE_MIN;
        for (int i = 0; i < explicitBoundsFromResult.size(); i++) {
            assertThat(explicitBoundsFromResult.get(i), equalTo(bucketVal));
            bucketVal += TEST_VALUE_RANGE_STEP;
        }
        final List<Map<String, Object>> bucketsFromResult = (ArrayList<Map<String, Object>>)result.get(0).toMap().get("buckets");
        double expectedBucketMin = -Float.MAX_VALUE;
        double expectedBucketMax = TEST_VALUE_RANGE_MIN;
        for (int i = 0; i < bucketsFromResult.size(); i++) {
            assertThat(bucketsFromResult.get(i), hasEntry("min", expectedBucketMin));
            assertThat(bucketsFromResult.get(i), hasEntry("max", expectedBucketMax));
            assertThat(bucketsFromResult.get(i), hasEntry("count", expectedBucketCounts[i]));
            expectedBucketMin = expectedBucketMax;
            expectedBucketMax += TEST_VALUE_RANGE_STEP;
            if (i == bucketsFromResult.size()-2) {
                expectedBucketMax = Float.MAX_VALUE;
            }
        }

        assertThat(result.get(0).get("startTime", String.class), equalTo(testTime.toString()));
        assertThat(result.get(0).get("time", String.class), equalTo(testTime.plusSeconds(100).toString()));
    }

    @Test
    void testHistogramAggregateOTelFormat_with_startTime_before_currentTime_and_all_other_times_after_that_has_the_correct_startTime() {
        histogramAggregateActionConfig = mock(HistogramAggregateActionConfig.class);
        String startTimeKey = UUID.randomUUID().toString();
        String endTimeKey = UUID.randomUUID().toString();
        when(histogramAggregateActionConfig.getStartTimeKey()).thenReturn(startTimeKey);
        when(histogramAggregateActionConfig.getEndTimeKey()).thenReturn(endTimeKey);
        final String testName = UUID.randomUUID().toString();
        when(histogramAggregateActionConfig.getMetricName()).thenReturn(testName);
        when(histogramAggregateActionConfig.getOutputFormat()).thenReturn(OutputFormat.OTEL_METRICS.toString());
        final String testUnits = "ms";
        when(histogramAggregateActionConfig.getUnits()).thenReturn(testUnits);
        when(histogramAggregateActionConfig.getRecordMinMax()).thenReturn(true);
        final double TEST_VALUE_RANGE_MIN = 0.0;
        final double TEST_VALUE_RANGE_MAX = 6.0;
        final double TEST_VALUE_RANGE_STEP = 2.0;
        final double bucket1 = TEST_VALUE_RANGE_MIN;
        final double bucket2 = bucket1 + TEST_VALUE_RANGE_STEP;
        final double bucket3 = bucket2 + TEST_VALUE_RANGE_STEP;
        List<Number> buckets = new ArrayList<Number>();
        buckets.add(bucket1);
        buckets.add(bucket2);
        buckets.add(bucket3);
        when(histogramAggregateActionConfig.getBuckets()).thenReturn(buckets);
        final String testKey = RandomStringUtils.randomAlphabetic(10);
        when(histogramAggregateActionConfig.getKey()).thenReturn(testKey);
        final String testPrefix = RandomStringUtils.randomAlphabetic(7);
        when(histogramAggregateActionConfig.getSumKey()).thenReturn(testPrefix+"sum");
        when(histogramAggregateActionConfig.getMinKey()).thenReturn(testPrefix+"min");
        when(histogramAggregateActionConfig.getMaxKey()).thenReturn(testPrefix+"max");
        when(histogramAggregateActionConfig.getCountKey()).thenReturn(testPrefix+"count");
        when(histogramAggregateActionConfig.getBucketsKey()).thenReturn(testPrefix+"buckets");
        when(histogramAggregateActionConfig.getBucketCountsKey()).thenReturn(testPrefix+"bucketcounts");
        when(histogramAggregateActionConfig.getDurationKey()).thenReturn(testPrefix+"duration");
        histogramAggregateAction = createObjectUnderTest();
        final String dataKey = RandomStringUtils.randomAlphabetic(10);
        final String dataValue = RandomStringUtils.randomAlphabetic(15);
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Map.of(dataKey, dataValue));

        final Instant expectedFirstStartTime = Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(2, ChronoUnit.SECONDS);
        final double value1 = ThreadLocalRandom.current().nextDouble(TEST_VALUE_RANGE_MIN-TEST_VALUE_RANGE_STEP, TEST_VALUE_RANGE_MAX+TEST_VALUE_RANGE_STEP);
        Map<Object, Object> eventMapEarliest = Collections.synchronizedMap(Map.of(testKey, value1, startTimeKey, expectedFirstStartTime, endTimeKey, expectedFirstStartTime));
        Event testEventEarliest = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMapEarliest)
                .build();
        final AggregateActionResponse aggregateActionResponse = histogramAggregateAction.handleEvent(testEventEarliest, aggregateActionInput);
        assertThat(aggregateActionResponse.getEvent(), equalTo(null));

        final Instant laterStartTime = Instant.now().truncatedTo(ChronoUnit.SECONDS).plus(5, ChronoUnit.SECONDS);
        final double value2 = ThreadLocalRandom.current().nextDouble(TEST_VALUE_RANGE_MIN-TEST_VALUE_RANGE_STEP, TEST_VALUE_RANGE_MAX+TEST_VALUE_RANGE_STEP);
        Map<Object, Object> eventMapLater = Collections.synchronizedMap(Map.of(testKey, value2, startTimeKey, laterStartTime, endTimeKey, laterStartTime));
        Event testEventLater = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMapLater)
                .build();

        final AggregateActionResponse aggregateActionResponseLater = histogramAggregateAction.handleEvent(testEventLater, aggregateActionInput);
        assertThat(aggregateActionResponseLater.getEvent(), equalTo(null));

        final AggregateActionOutput actionOutput = histogramAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertThat(result.size(), equalTo(1));

        assertThat(result.get(0).toMap(), hasKey("startTime"));
        assertThat(result.get(0).toMap(), hasKey("time"));

        final String actualStartTime = result.get(0).get("startTime", String.class);
        assertThat(actualStartTime, notNullValue());
        final Instant startTimeInstant = Instant.parse(actualStartTime).truncatedTo(ChronoUnit.MILLIS);
        assertThat(startTimeInstant, equalTo(expectedFirstStartTime));

        final String actualTime = result.get(0).get("time", String.class);
        assertThat(actualTime, notNullValue());
        final Instant actualTimeInstant = Instant.parse(actualTime).truncatedTo(ChronoUnit.MILLIS);
        assertThat(actualTimeInstant, equalTo(laterStartTime));
    }
}
