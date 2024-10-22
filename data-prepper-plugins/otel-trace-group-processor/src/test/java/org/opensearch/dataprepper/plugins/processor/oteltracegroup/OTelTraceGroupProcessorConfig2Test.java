package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.processor.oteltracegroup.ConnectionConfiguration2.HOSTS;

class OTelTraceGroupProcessorConfig2Test {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");

    @Test
    void testDeserialize() {
        final Map<String, Object> pluginSetting = Map.of(HOSTS, TEST_HOSTS);
        final OTelTraceGroupProcessorConfig2 objectUnderTest = OBJECT_MAPPER.convertValue(
                pluginSetting, OTelTraceGroupProcessorConfig2.class);
        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getEsConnectionConfig(), notNullValue());
        assertThat(objectUnderTest.getEsConnectionConfig().getHosts(), equalTo(TEST_HOSTS));
    }
}