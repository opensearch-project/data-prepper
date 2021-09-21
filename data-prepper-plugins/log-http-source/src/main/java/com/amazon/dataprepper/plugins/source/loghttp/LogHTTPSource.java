/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.loghttp;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.throttling.ThrottlingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@DataPrepperPlugin(name = "otel_trace_source", type = PluginType.SOURCE)
public class LogHTTPSource implements Source<Record<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(LogHTTPSource.class);

    private final LogHTTPSourceConfig logHTTPSourceConfig;
    private Server server;

    public LogHTTPSource(PluginSetting pluginSetting) {
        logHTTPSourceConfig = LogHTTPSourceConfig.buildConfig(pluginSetting);
    }

    @Override
    public void start(Buffer<Record<String>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        if (server == null) {
            final ServerBuilder sb = Server.builder();
            // TODO: allow tls/ssl
            sb.http(logHTTPSourceConfig.getPort());
            sb.maxNumConnections(logHTTPSourceConfig.getMaxConnectionCount());
            final int threads = logHTTPSourceConfig.getThreadCount();
            final ScheduledThreadPoolExecutor blockingTaskExecutor = new ScheduledThreadPoolExecutor(threads);
            sb.blockingTaskExecutor(blockingTaskExecutor, true);
            // TODO: add throttling
//            final int maxPendingRequests = logHTTPSourceConfig.getMaxPendingRequests();
//            final LogThrottlingStrategy logThrottlingStrategy = new LogThrottlingStrategy(
//                    maxPendingRequests, blockingTaskExecutor.getQueue());
//            final LogThrottlingRejectHandler logThrottlingRejectHandler = new LogThrottlingRejectHandler(maxPendingRequests);
//            sb.decorator(ThrottlingService.newDecorator(logThrottlingStrategy, logThrottlingRejectHandler));
//            final LogHTTPService logHTTPService = new LogHTTPService(logHTTPSourceConfig.getRequestTimeoutInMillis(), buffer);
//            sb.annotatedService(logHTTPService);

            server = sb.build();
        }

        try {
            server.start().get();
        } catch (ExecutionException ex) {
            if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            } else {
                throw new RuntimeException(ex);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
        LOG.info("Started log_http_source...");
    }

    @Override
    public void stop() {
        if (server != null) {
            try {
                server.stop().get();
            } catch (ExecutionException ex) {
                if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) ex.getCause();
                } else {
                    throw new RuntimeException(ex);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }
        LOG.info("Stopped log_http_source.");
    }
}
