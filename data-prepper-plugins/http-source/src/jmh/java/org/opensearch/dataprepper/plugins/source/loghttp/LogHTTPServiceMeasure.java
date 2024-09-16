/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.ServiceRequestContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class LogHTTPServiceMeasure {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private HttpData httpData;
        private Buffer buffer;
        private LogHTTPService logHTTPService;
        private ServiceRequestContext serviceRequestContext;
        private RequestHeaders requestHeaders;

        @Setup
        public void setUp() throws IOException {
            byte[] jsonContent = new TestGenerator().createJson(10 * 1024 * 1024);
            httpData = HttpData.ofUtf8(new String(jsonContent));

            buffer = mock(Buffer.class, withSettings().stubOnly());
            when(buffer.isByteBuffer()).thenReturn(true);
            when(buffer.getMaxRequestSize()).thenReturn(Optional.of(512 * 1024));
            when(buffer.getOptimalRequestSize()).thenReturn(Optional.of(256 * 1024));

            serviceRequestContext = mock(ServiceRequestContext.class);
            logHTTPService = new LogHTTPService((int) Duration.ofSeconds(10).toMillis(), buffer, PluginMetrics.fromPrefix("testing"));

            requestHeaders = RequestHeaders.builder()
                    .method(HttpMethod.POST)
                    .path("/test")
                    .build();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 1)
    @Measurement(iterations = 5, time = 10)
    public HttpResponse measure_doPost(BenchmarkState benchmarkState) throws Exception {
        AggregatedHttpRequest aggregatedHttpRequest = AggregatedHttpRequest.of(benchmarkState.requestHeaders, benchmarkState.httpData);
        return benchmarkState.logHTTPService.doPost(benchmarkState.serviceRequestContext, aggregatedHttpRequest);
    }
}
