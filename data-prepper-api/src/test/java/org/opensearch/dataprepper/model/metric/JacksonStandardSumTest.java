/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import org.junit.jupiter.api.Test;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import java.util.UUID;

public class JacksonStandardSumTest extends JacksonSumTest {
    private JacksonSum jacksonSum;

    public JacksonSum createObjectUnderTest(Map<String, Object> attributes) {
        return JacksonStandardSum.builder()
                .withAttributes(attributes)
                .withName(TEST_NAME)
                .withDescription(TEST_DESCRIPTION)
                .withEventKind(TEST_EVENT_KIND)
                .withStartTime(TEST_START_TIME)
                .withTime(TEST_TIME)
                .withUnit(TEST_UNIT_NAME)
                .withIsMonotonic(TEST_IS_MONOTONIC)
                .withAggregationTemporality(TEST_AGGREGATION_TEMPORALITY)
                .withValue(TEST_VALUE)
                .withServiceName(TEST_SERVICE_NAME)
                .withSchemaUrl(TEST_SCHEMA_URL)
                .build();
    }

    @Test
    @Override
    public void testSumJsonToString() {
        String attrKey = UUID.randomUUID().toString();
        String attrVal = UUID.randomUUID().toString();
        jacksonSum = createObjectUnderTest(Map.of(attrKey, attrVal));
        assertThat(jacksonSum, instanceOf(JacksonStandardSum.class));
        final String jsonResult = jacksonSum.toJsonString();
        String attrString = String.format("\"attributes\":{\"%s\":\"%s\"}", attrKey, attrVal);

        assertThat(jsonResult.indexOf(attrString), not(equalTo(-1)));
    }
}



