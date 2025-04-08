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

public class JacksonStandardExponentialHistogramTest extends JacksonExponentialHistogramTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private JacksonExponentialHistogram jacksonExponentialHistogram;

    public JacksonExponentialHistogram createObjectUnderTest(Map<String, Object> attributes) {
        return JacksonStandardExponentialHistogram.builder()
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
                .withNegativeBuckets(TEST_NEGATIVE_BUCKETS)
                .withPositiveBuckets(TEST_POSITIVE_BUCKETS)
                .withAggregationTemporality(TEST_AGGREGATION_TEMPORALITY)
                .withSchemaUrl(TEST_SCHEMA_URL)
                .withScale(TEST_SCALE)
                .withZeroCount(TEST_ZERO_COUNT)
                .withZeroThreshold(TEST_ZERO_THRESHOLD)
                .withMin(TEST_MIN)
                .withMax(TEST_MAX)
                .withPositiveOffset(TEST_POSITIVE_OFFSET)
                .withNegativeOffset(TEST_NEGATIVE_OFFSET)
                .withNegative(TEST_NEGATIVE)
                .withPositive(TEST_POSITIVE)
                .build();
    }

    @Test
    @Override
    public void testHistogramToJsonString() throws Exception {
        jacksonExponentialHistogram = createObjectUnderTest(null);
        assertThat(jacksonExponentialHistogram, instanceOf(JacksonStandardExponentialHistogram.class));
        final String jsonResult = jacksonExponentialHistogram.toJsonString();
        final Map<String, Object> resultMap = mapper.readValue(jsonResult, new TypeReference<Map<String, Object>>() {});

        assertThat(resultMap.containsKey("attributes"), is(true));
        assertThat(resultMap.get("attributes"), equalTo(Map.of()));
    }
    @Test
    @Override
    public void testHistogramToJsonStringWithAttributes() throws JSONException {
        jacksonExponentialHistogram = createObjectUnderTest(TEST_ATTRIBUTES);
        String result = jacksonExponentialHistogram.toJsonString();
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/standard_exponential_histogram.json"));
        String expected = String.format(file, TEST_START_TIME, TEST_TIME, TEST_KEY1_TIME, TEST_KEY2);
        JSONAssert.assertEquals(expected, result, false);
    }
    
}
