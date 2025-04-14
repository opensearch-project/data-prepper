/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import com.google.common.collect.ImmutableMap;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.AggregateEventHandle;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JacksonSummaryTest {

    protected static final Map<String, Object> TEST_ATTRIBUTES = ImmutableMap.of(
            "key1", new Date().getTime(),
            "key2", UUID.randomUUID().toString());
    protected static final String TEST_SERVICE_NAME = "service";
    protected static final String TEST_NAME = "name";
    protected static final String TEST_DESCRIPTION = "description";
    protected static final String TEST_UNIT_NAME = "unit";
    protected static final String TEST_START_TIME = UUID.randomUUID().toString();
    protected static final String TEST_TIME = UUID.randomUUID().toString();
    protected static final String TEST_EVENT_KIND = Metric.KIND.SUMMARY.name();
    protected static final Double TEST_SUM = 1D;
    protected static final List<Quantile> TEST_QUANTILES = Arrays.asList(
            new DefaultQuantile(0.4, 0.5),
            new DefaultQuantile(0.2, 0.6)
    );
    protected static final Integer TEST_QUANTILES_COUNT = 2;
    protected static final Long TEST_COUNT = 2L;
    protected static final String TEST_SCHEMA_URL = "schema";

    private JacksonSummary summary;

    private JacksonSummary.Builder builder;

    @BeforeEach
    public void setup() {
        builder = JacksonSummary.builder()
                .withAttributes(TEST_ATTRIBUTES)
                .withName(TEST_NAME)
                .withDescription(TEST_DESCRIPTION)
                .withEventKind(TEST_EVENT_KIND)
                .withStartTime(TEST_START_TIME)
                .withTime(TEST_TIME)
                .withUnit(TEST_UNIT_NAME)
                .withServiceName(TEST_SERVICE_NAME)
                .withSum(TEST_SUM)
                .withQuantiles(TEST_QUANTILES)
                .withCount(TEST_COUNT)
                .withQuantilesValueCount(TEST_QUANTILES_COUNT)
                .withSchemaUrl(TEST_SCHEMA_URL);

        summary = builder.build();
    }

    @Test
    public void testGetAttributes() {
        final Map<String, Object> attributes = summary.getAttributes();
        TEST_ATTRIBUTES.keySet().forEach(key -> {
                    assertThat(attributes, hasKey(key));
                    assertThat(attributes.get(key), is(equalTo(TEST_ATTRIBUTES.get(key))));
                }
        );
    }

    @Test
    public void testGeTAttributes_withNull_mustBeEmpty() {
        builder.withAttributes(null);
        JacksonSummary summary = builder.build();
        assertThat(summary.getAttributes(),is(anEmptyMap()));
    }

    @Test
    public void testGetName() {
        final String name = summary.getName();
        assertThat(name, is(equalTo(TEST_NAME)));
    }

    @Test
    public void testGetDefaultEventHandle() {
        EventHandle eventHandle = new DefaultEventHandle(Instant.now());
        builder.withEventHandle(eventHandle);
        summary = builder.build();
        final EventHandle handle = summary.getEventHandle();
        assertThat(handle, is(sameInstance(eventHandle)));
    }

    @Test
    public void testGetAggregateEventHandle() {
        EventHandle eventHandle = new AggregateEventHandle(Instant.now());
        builder.withEventHandle(eventHandle);
        summary = builder.build();
        final EventHandle handle = summary.getEventHandle();
        assertThat(handle, is(sameInstance(eventHandle)));
    }

    @Test
    public void testGetDescription() {
        final String description = summary.getDescription();
        assertThat(description, is(equalTo(TEST_DESCRIPTION)));
    }

    @Test
    public void testGetKind() {
        final String kind = summary.getKind();
        assertThat(kind, is(equalTo(TEST_EVENT_KIND)));
    }

    @Test
    public void testGetServiceName() {
        final String name = summary.getServiceName();
        assertThat(name, is(equalTo(TEST_SERVICE_NAME)));
    }

    @Test
    public void testGetSum() {
        final Double sum = summary.getSum();
        assertThat(sum, is(equalTo(TEST_SUM)));
    }

    @Test
    public void testGetTimeReceived() {
        Instant now = Instant.now();
        builder.withTimeReceived(now);
        summary = builder.build();
        assertThat(((DefaultEventHandle)summary.getEventHandle()).getInternalOriginationTime(), is(now));
    }


    @Test
    public void testGetCount() {
        final Long count = summary.getCount();
        assertThat(count, is(equalTo(TEST_COUNT)));
    }

    @Test
    public void testGetQuantilesName() {
        final List<? extends Quantile> quantiles = summary.getQuantiles();
        assertThat(quantiles.size(), is(equalTo(2)));
        Quantile firstQuantile = quantiles.get(0);
        Quantile secondQuantile = quantiles.get(1);
        assertThat(firstQuantile.getQuantile(), is(equalTo(0.4)));
        assertThat(firstQuantile.getValue(), is(equalTo(0.5)));
        assertThat(secondQuantile.getQuantile(), is(equalTo(0.2)));
        assertThat(secondQuantile.getValue(), is(equalTo(0.6)));
    }

    @Test
    public void testGetQuantilesValuesCount() {
        final Integer quantilesValuesCount = summary.getQuantileValuesCount();
        assertThat(quantilesValuesCount, is(equalTo(TEST_QUANTILES_COUNT)));
    }

    @Test
    public void testBuilder_missingNonNullParameters_throwsNullPointerException() {
        final JacksonSum.Builder builder = JacksonSum.builder();
        builder.withValue(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void testBuilder_withEmptyTime_throwsIllegalArgumentException() {
        builder.withTime("");
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    public void testGetSchemaUrl() {
        final String url = summary.getSchemaUrl();
        assertThat(url, Matchers.is(Matchers.equalTo(TEST_SCHEMA_URL)));
    }
    
    @Test
    public void testSummaryJsonToString() {
        String attrKey = UUID.randomUUID().toString();
        String attrVal = UUID.randomUUID().toString();
        final Map<String, Object> attributes = Map.of(attrKey, attrVal);
        summary.put("attributes", attributes);
        final String resultAttr = summary.toJsonString();
        String attrString = String.format("\"attributes\":{\"%s\":\"%s\"}", attrKey, attrVal);
        assertThat(resultAttr.indexOf(attrString), equalTo(-1));
    }

    @Test
    public void testSummaryJsonToStringWithAttributes() {
        summary = builder.build(false);
        String attrKey = UUID.randomUUID().toString();
        String attrVal = UUID.randomUUID().toString();
        final Map<String, Object> attributes = Map.of(attrKey, attrVal);
        summary.put("attributes", attributes);
        final String resultAttr = summary.toJsonString();
        String attrString = String.format("\"attributes\":{\"%s\":\"%s\"}", attrKey, attrVal);
        assertThat(resultAttr.indexOf(attrString), not(equalTo(-1)));
    }

}
