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

public class JacksonStandardGaugeTest extends JacksonGaugeTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private JacksonGauge jacksonGauge;

    public JacksonGauge createObjectUnderTest(Map<String, Object> attributes) {
        return JacksonStandardGauge.builder()
                .withAttributes(attributes)
                .withName(TEST_NAME)
                .withDescription(TEST_DESCRIPTION)
                .withEventKind(TEST_EVENT_KIND)
                .withStartTime(TEST_START_TIME)
                .withTime(TEST_TIME)
                .withUnit(TEST_UNIT_NAME)
                .withScope(TEST_SCOPE)
                .withResource(TEST_RESOURCE)
                .withValue(TEST_VALUE)
                .withServiceName(TEST_SERVICE_NAME)
                .withExemplars(TEST_EXEMPLARS)
                .withSchemaUrl(TEST_SCHEMA_URL)
                .withFlags(TEST_FLAGS)
                .build();
    }

    @Test
    @Override
    public void testGaugeToJsonString() throws Exception {
        jacksonGauge = createObjectUnderTest(null);
        assertThat(jacksonGauge, instanceOf(JacksonStandardGauge.class));
        final String jsonResult = jacksonGauge.toJsonString();
        final Map<String, Object> resultMap = mapper.readValue(jsonResult, new TypeReference<Map<String, Object>>() {});

        assertThat(resultMap.containsKey("attributes"), is(true));
        assertThat(resultMap.get("attributes"), equalTo(Map.of()));
    }
    @Test
    @Override
    public void testGaugeToJsonStringWithAttributes() throws JSONException {
        jacksonGauge = createObjectUnderTest(TEST_ATTRIBUTES);
        String result = jacksonGauge.toJsonString();
        String file = IOUtils.toString(this.getClass().getResourceAsStream("/testjson/standard_gauge.json"));
        String expected = String.format(file, TEST_TIME_KEY1, TEST_KEY2);
        JSONAssert.assertEquals(expected, result, false);
    }
    
}


