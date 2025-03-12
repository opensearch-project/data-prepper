/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import java.util.UUID;

public class JacksonStandardSummaryTest extends JacksonSummaryTest {
    private JacksonSummary jacksonSummary;

    public JacksonSummary createObjectUnderTest(Map<String, Object> attributes) {
        return JacksonStandardSummary.builder()
                .withAttributes(attributes)
                .withName(TEST_NAME)
                .withDescription(TEST_DESCRIPTION)
                .withEventKind(TEST_EVENT_KIND)
                .withStartTime(TEST_START_TIME)
                .withTime(TEST_TIME)
                .withSum(TEST_SUM)
                .withCount(TEST_COUNT)
                .withUnit(TEST_UNIT_NAME)
                .withServiceName(TEST_SERVICE_NAME)
                .withSchemaUrl(TEST_SCHEMA_URL)
                .withQuantiles(TEST_QUANTILES)
                .withQuantilesValueCount(TEST_QUANTILES_COUNT)
                .build();
    }

    @Test
    @Override
    public void testSummaryJsonToString() {
        String attrKey = UUID.randomUUID().toString();
        String attrVal = UUID.randomUUID().toString();
        jacksonSummary = createObjectUnderTest(Map.of(attrKey, attrVal));
        assertThat(jacksonSummary, instanceOf(JacksonStandardSummary.class));
        final String jsonResult = jacksonSummary.toJsonString();
        String attrString = String.format("\"attributes\":{\"%s\":\"%s\"}", attrKey, attrVal);

        assertThat(jsonResult.indexOf(attrString), not(equalTo(-1)));
    }
}



