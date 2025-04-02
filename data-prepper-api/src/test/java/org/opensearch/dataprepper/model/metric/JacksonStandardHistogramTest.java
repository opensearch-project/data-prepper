/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;
import io.micrometer.core.instrument.util.IOUtils;

import org.junit.jupiter.api.Test;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class JacksonStandardHistogramTest extends JacksonHistogramTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private JacksonHistogram jacksonHistogram;

    public JacksonHistogram createObjectUnderTest(Map<String, Object> attributes) {
        return JacksonStandardHistogram.builder()
                .withAttributes(attributes)
                .withName(TEST_NAME)
                .withDescription(TEST_DESCRIPTION)
                .withEventKind(TEST_EVENT_KIND)
                .withStartTime(TEST_START_TIME)
                .withTime(TEST_TIME)
                .withUnit(TEST_UNIT_NAME)
                .withServiceName(TEST_SERVICE_NAME)
                .withSum(TEST_SUM)
                .withCount(TEST_COUNT)
                .withBucketCount(TEST_BUCKETS_COUNT)
                .withExplicitBoundsCount(TEST_EXPLICIT_BOUNDS_COUNT)
                .withBuckets(TEST_BUCKETS)
                .withAggregationTemporality(TEST_AGGREGATION_TEMPORALITY)
                .withSchemaUrl(TEST_SCHEMA_URL)
                .withExplicitBoundsList(TEST_EXPLICIT_BOUNDS_LIST)
                .withBucketCountsList(TEST_BUCKET_COUNTS_LIST)
                .withMin(TEST_MIN)
                .withMax(TEST_MAX)
                .build();
    }

    @Test
    @Override
    public void testHistogramToJsonString() throws Exception {
        jacksonHistogram = createObjectUnderTest(null);
        assertThat(jacksonHistogram, instanceOf(JacksonStandardHistogram.class));
        final String jsonResult = jacksonHistogram.toJsonString();
        final Map<String, Object> resultMap = mapper.readValue(jsonResult, new TypeReference<Map<String, Object>>() {});

        assertThat(resultMap.containsKey("attributes"), is(true));
        assertThat(resultMap.get("attributes"), equalTo(Map.of()));
    }
    @Test
    @Override
    public void testHistogramToJsonStringWithAttributes() throws JSONException {
        jacksonHistogram = createObjectUnderTest(TEST_ATTRIBUTES);
        String result = jacksonHistogram.toJsonString();
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/standard_histogram.json"));
        String expected = String.format(file, TEST_START_TIME, TEST_TIME, TEST_KEY1_TIME, TEST_KEY2);
        JSONAssert.assertEquals(expected, result, false);
    }
    
}

