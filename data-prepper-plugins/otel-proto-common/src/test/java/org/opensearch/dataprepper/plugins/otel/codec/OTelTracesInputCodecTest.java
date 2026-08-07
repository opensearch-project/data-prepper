/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;
import org.opensearch.dataprepper.test.plugins.PluginConfigurationFile;
import org.opensearch.dataprepper.test.plugins.junit.BaseDataPrepperPluginStandardTestSuite;

import java.io.InputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@DataPrepperPluginTest(pluginName = "otel_traces", pluginType = InputCodec.class)
class OTelTracesInputCodecTest extends BaseDataPrepperPluginStandardTestSuite {

    private static final String TEST_REQUEST_TRACES_FILE = "test-request-multiple-traces.json";

    @Test
    void parse_produces_valid_spans(
            @PluginConfigurationFile("otel_traces_codec_json_format.yaml") final InputCodec codec) throws Exception {
        InputStream inputStream = OTelTracesInputCodecTest.class.getClassLoader().getResourceAsStream(TEST_REQUEST_TRACES_FILE);
        codec.parse(inputStream, record -> {
            Span span = (Span) record.getData();
            assertThat(span.getServiceName(), is("analytics-service1"));
            assertThat(span.getTraceId().isEmpty(), is(false));
            assertThat(span.getSpanId().isEmpty(), is(false));
            assertThat(span.getName().isEmpty(), is(false));
            assertThat(span.getStartTime().isEmpty(), is(false));
            assertThat(span.getEndTime().isEmpty(), is(false));
        });
    }
}