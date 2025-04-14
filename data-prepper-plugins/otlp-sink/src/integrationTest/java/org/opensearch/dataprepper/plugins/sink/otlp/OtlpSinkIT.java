/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.JacksonStandardSpan;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.http.OtlpHttpSender;
import software.amazon.awssdk.regions.Region;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test for OtlpSink. Requires AWS credentials to be set up in the environment.
 * This will not run as part of the Data Prepper build.
 * <p>
 * ./gradlew :data-prepper-plugins:otlp-sink:integrationTest \
 * -Dtests.xray.region=us-west-2 \
 * -Dtests.xray.profile=dev
 */
class OtlpSinkIT {

    private OtlpSinkConfig mockConfig;
    private OtlpSink target;

    @BeforeEach
    void setUp() {
        System.setProperty("aws.accessKeyId", "dummy");
        System.setProperty("aws.secretAccessKey", "dummy");
        System.setProperty("aws.region", Region.US_WEST_2.toString());

        mockConfig = mock(OtlpSinkConfig.class);
        when(mockConfig.getMaxRetries()).thenReturn(3);
        when(mockConfig.getBatchSize()).thenReturn(100);

        target = new OtlpSink(mockConfig);
    }

    @AfterEach
    void cleanUp() {
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.region");
    }

    /**
     * This test is not part of the Data Prepper build. It requires AWS credentials to be set up in the environment.
     *
     * @throws InterruptedException
     */
    @Test
    void testSinkProcessesHardcodedSpan() {
        final Span testSpan = JacksonStandardSpan.builder()
                .withTraceId("0123456789abcdef0123456789abcdef")
                .withSpanId("0123456789abcdef")
                .withParentSpanId("1111111111111111")
                .withName("my-test-span")
                .withStartTime(Instant.now().toString())
                .withEndTime(Instant.now().plusMillis(10).toString())
                .withAttributes(Collections.emptyMap())
                .withKind("INTERNAL")
                .build();

        final Record<Span> record = new Record<>(testSpan);
        final OtlpSink sink = new OtlpSink(mockConfig, mock(OTelProtoStandardCodec.OTelProtoEncoder.class), mock(OtlpHttpSender.class));

        sink.initialize();
        sink.output(List.of(record));
        sink.shutdown();
    }
}
