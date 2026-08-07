/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PrometheusOutputConsistencyTest {

    private static final long TIMESTAMP_MS = 1395066363000L;

    @Mock
    private PrometheusRemoteWriteSourceConfig config;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void gauge_output_is_consistent_across_parsers(final boolean flattenLabels) throws PrometheusParseException {
        final String textBody = "# TYPE temperature_celsius gauge\n" +
                "temperature_celsius{location=\"outside\"} 21.3 " + TIMESTAMP_MS + "\n";

        final Remote.WriteRequest writeRequest = Remote.WriteRequest.newBuilder()
                .addTimeseries(Types.TimeSeries.newBuilder()
                        .addLabels(Types.Label.newBuilder().setName("__name__").setValue("temperature_celsius").build())
                        .addLabels(Types.Label.newBuilder().setName("location").setValue("outside").build())
                        .addSamples(Types.Sample.newBuilder().setValue(21.3).setTimestamp(TIMESTAMP_MS).build())
                        .build())
                .build();

        final List<Record<Event>> textRecords = new TextExpositionParser(flattenLabels).parse(textBody);
        final List<Record<Event>> protoRecords = parseProtobuf(writeRequest, flattenLabels);

        assertThat(textRecords, hasSize(1));
        assertThat(protoRecords, hasSize(1));

        final Metric textMetric = (Metric) textRecords.get(0).getData();
        final Metric protoMetric = (Metric) protoRecords.get(0).getData();

        assertThat(protoMetric.getKind(), equalTo(textMetric.getKind()));
        assertThat(protoMetric.getName(), equalTo("temperature_celsius"));
        assertThat(textMetric.getName(), equalTo("temperature_celsius"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void counter_output_is_consistent_across_parsers(final boolean flattenLabels) throws PrometheusParseException {
        final String textBody = "# TYPE http_requests_total counter\n" +
                "http_requests_total{method=\"GET\",code=\"200\"} 1027 " + TIMESTAMP_MS + "\n";

        final Remote.WriteRequest writeRequest = Remote.WriteRequest.newBuilder()
                .addTimeseries(Types.TimeSeries.newBuilder()
                        .addLabels(Types.Label.newBuilder().setName("__name__").setValue("http_requests_total").build())
                        .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                        .addLabels(Types.Label.newBuilder().setName("code").setValue("200").build())
                        .addSamples(Types.Sample.newBuilder().setValue(1027.0).setTimestamp(TIMESTAMP_MS).build())
                        .build())
                .build();

        final List<Record<Event>> textRecords = new TextExpositionParser(flattenLabels).parse(textBody);
        final List<Record<Event>> protoRecords = parseProtobuf(writeRequest, flattenLabels);

        assertThat(textRecords, hasSize(1));
        assertThat(protoRecords, hasSize(1));

        final Metric textMetric = (Metric) textRecords.get(0).getData();
        final Metric protoMetric = (Metric) protoRecords.get(0).getData();

        assertThat(protoMetric.getKind(), equalTo(textMetric.getKind()));
        assertThat(protoMetric.getName(), equalTo("http_requests"));
        assertThat(textMetric.getName(), equalTo("http_requests"));
    }

    private List<Record<Event>> parseProtobuf(final Remote.WriteRequest writeRequest, final boolean flattenLabels)
            throws PrometheusParseException {
        when(config.isFlattenLabels()).thenReturn(flattenLabels);
        final RemoteWriteProtobufParser parser = new RemoteWriteProtobufParser(config);
        return parser.parseDecompressed(writeRequest.toByteArray());
    }
}
