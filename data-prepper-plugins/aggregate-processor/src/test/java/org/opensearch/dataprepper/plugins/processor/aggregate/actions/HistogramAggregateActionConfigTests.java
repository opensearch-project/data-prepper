/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import org.junit.jupiter.api.extension.ExtendWith; 
import org.mockito.junit.jupiter.MockitoExtension;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.UUID;

import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.HistogramAggregateActionConfig.DEFAULT_GENERATED_KEY_PREFIX;

import java.util.concurrent.ThreadLocalRandom;
import java.util.List;
import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@ExtendWith(MockitoExtension.class)
public class HistogramAggregateActionConfigTests {
    private HistogramAggregateActionConfig histogramAggregateActionConfig;

    private HistogramAggregateActionConfig createObjectUnderTest() {
        return new HistogramAggregateActionConfig();
    }
    
    @BeforeEach
    void setup() {
        histogramAggregateActionConfig = createObjectUnderTest();
    }
    
    @Test
    void testDefault() {
        assertThat(histogramAggregateActionConfig.getGeneratedKeyPrefix(), equalTo(DEFAULT_GENERATED_KEY_PREFIX));
        assertThat(histogramAggregateActionConfig.getRecordMinMax(), equalTo(false));
        assertThat(histogramAggregateActionConfig.getOutputFormat(), equalTo(OutputFormat.OTEL_METRICS));
        assertThat(histogramAggregateActionConfig.getMetricName(), equalTo(HistogramAggregateActionConfig.HISTOGRAM_METRIC_NAME));
    }

    @Test
    void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        final String testGeneratedKeyPrefix = RandomStringUtils.randomAlphabetic(10);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "generatedKeyPrefix", testGeneratedKeyPrefix);
        assertThat(histogramAggregateActionConfig.getGeneratedKeyPrefix(), equalTo(testGeneratedKeyPrefix));
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "recordMinMax", true);
        assertThat(histogramAggregateActionConfig.getRecordMinMax(), equalTo(true));
        final OutputFormat testOutputFormat = OutputFormat.OTEL_METRICS;
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "outputFormat", testOutputFormat);
        assertThat(histogramAggregateActionConfig.getOutputFormat(), equalTo(OutputFormat.OTEL_METRICS));
        final String testKey = RandomStringUtils.randomAlphabetic(10);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "key", testKey);
        assertThat(histogramAggregateActionConfig.getKey(), equalTo(testKey));
        final String testUnits = RandomStringUtils.randomAlphabetic(10);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "units", testUnits);
        assertThat(histogramAggregateActionConfig.getUnits(), equalTo(testUnits));
        final List<Double> doubleBuckets = new ArrayList<Double>();
        double doubleValue1 = ThreadLocalRandom.current().nextDouble();
        double doubleValue2 = ThreadLocalRandom.current().nextDouble();
        doubleBuckets.add(doubleValue1);
        doubleBuckets.add(doubleValue2);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", doubleBuckets);
        assertThat(histogramAggregateActionConfig.getBuckets(), containsInAnyOrder(doubleBuckets.toArray()));

        final List<Integer> intBuckets = new ArrayList<Integer>();
        int intValue1 = ThreadLocalRandom.current().nextInt();
        int intValue2 = ThreadLocalRandom.current().nextInt();
        intBuckets.add(intValue1);
        intBuckets.add(intValue2);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", intBuckets);
        assertThat(histogramAggregateActionConfig.getBuckets(), containsInAnyOrder(intBuckets.toArray()));

        final List<Float> floatBuckets = new ArrayList<Float>();
        float floatValue1 = (float)ThreadLocalRandom.current().nextDouble();
        float floatValue2 = (float)ThreadLocalRandom.current().nextDouble();
        floatBuckets.add(floatValue1);
        floatBuckets.add(floatValue2);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", floatBuckets);
        assertThat(histogramAggregateActionConfig.getBuckets(), containsInAnyOrder(floatBuckets.toArray()));

        final List<Byte> byteBuckets = new ArrayList<Byte>();
        byte byteValue1 = (byte)ThreadLocalRandom.current().nextInt();
        byte byteValue2 = (byte)ThreadLocalRandom.current().nextInt();
        byteBuckets.add(byteValue1);
        byteBuckets.add(byteValue2);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", byteBuckets);
        assertThat(histogramAggregateActionConfig.getBuckets(), containsInAnyOrder(byteBuckets.toArray()));

        final List<Short> shortBuckets = new ArrayList<Short>();
        short shortValue1 = (short)ThreadLocalRandom.current().nextInt();
        short shortValue2 = (short)ThreadLocalRandom.current().nextInt();
        shortBuckets.add(shortValue1);
        shortBuckets.add(shortValue2);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", shortBuckets);
        assertThat(histogramAggregateActionConfig.getBuckets(), containsInAnyOrder(shortBuckets.toArray()));

        final List<Long> longBuckets = new ArrayList<Long>();
        long longValue1 = ThreadLocalRandom.current().nextLong();
        long longValue2 = ThreadLocalRandom.current().nextLong();
        longBuckets.add(longValue1);
        longBuckets.add(longValue2);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", longBuckets);
        assertThat(histogramAggregateActionConfig.getBuckets(), containsInAnyOrder(longBuckets.toArray()));
        final String testName = UUID.randomUUID().toString();
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "metricName", testName);
        assertThat(histogramAggregateActionConfig.getMetricName(), equalTo(testName));
    }

    @Test
    void testInvalidBucketsConfig() throws NoSuchFieldException, IllegalAccessException {
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", new ArrayList<Double>());
        assertThrows(IllegalArgumentException.class, () -> histogramAggregateActionConfig.getBuckets());
    }
}
