/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otellogs.http;

import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.throttling.ThrottlingService;
import org.opensearch.dataprepper.http.LogThrottlingRejectHandler;
import org.opensearch.dataprepper.http.LogThrottlingStrategy;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;
import org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;

public class HttpServiceConfigurator {

    private static final Logger LOG = LoggerFactory.getLogger(HttpServiceConfigurator.class);
    private static final String PIPELINE_NAME_PLACEHOLDER = "${pipelineName}";
    private static final int MAX_PENDING_REQUESTS = 1024;
    private static final RetryInfoConfig DEFAULT_RETRY_INFO =
            new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    private final OTelLogsSourceConfig config;
    private final PluginMetrics pluginMetrics;
    private final String pipelineName;

    public HttpServiceConfigurator(final OTelLogsSourceConfig config,
                                   final PluginMetrics pluginMetrics,
                                   final String pipelineName) {
        this.config = config;
        this.pluginMetrics = pluginMetrics;
        this.pipelineName = pipelineName;
    }

    public void configure(final ServerBuilder sb,
                          final Buffer<Record<Object>> buffer,
                          final BlockingQueue<Runnable> executorQueue) {
        final String path = config.getHttpPath().replace(PIPELINE_NAME_PLACEHOLDER, pipelineName);
        LOG.info("Configuring HTTP service at path: {}", path);

        final RetryInfoConfig retryInfo = config.getRetryInfo() != null
                ? config.getRetryInfo()
                : DEFAULT_RETRY_INFO;

        final OTelProtoCodec.OTelProtoDecoder decoder = config.getOutputFormat() == OTelOutputFormat.OPENSEARCH
                ? new OTelProtoOpensearchCodec.OTelProtoDecoder()
                : new OTelProtoStandardCodec.OTelProtoDecoder();

        final ArmeriaHttpService httpService = new ArmeriaHttpService(
                buffer, pluginMetrics, (int) (config.getRequestTimeoutInMillis() * 0.8), decoder);

        final HttpExceptionHandler exceptionHandler =
                new HttpExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());

        final LogThrottlingStrategy throttlingStrategy =
                new LogThrottlingStrategy(MAX_PENDING_REQUESTS, executorQueue);
        final LogThrottlingRejectHandler throttlingRejectHandler =
                new LogThrottlingRejectHandler(MAX_PENDING_REQUESTS, pluginMetrics);
        sb.decorator(path, ThrottlingService.newDecorator(throttlingStrategy, throttlingRejectHandler));

        if (CompressionOption.NONE.equals(config.getCompression())) {
            sb.annotatedService(path, httpService, exceptionHandler);
        } else {
            sb.annotatedService(path, httpService, DecodingService.newDecorator(), exceptionHandler);
        }
    }
}

