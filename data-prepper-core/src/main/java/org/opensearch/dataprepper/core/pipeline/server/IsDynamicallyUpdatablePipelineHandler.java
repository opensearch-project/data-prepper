/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.opensearch.dataprepper.core.pipeline.PipelinesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;

/**
 * HttpHandler to handle requests for updating pipeline configurations from S3
 */
public class IsDynamicallyUpdatablePipelineHandler extends UpdatePipelineBaseHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(IsDynamicallyUpdatablePipelineHandler.class);

    public IsDynamicallyUpdatablePipelineHandler(final PipelinesProvider pipelinesProvider) {
        super(pipelinesProvider);
    }

    public IsDynamicallyUpdatablePipelineHandler(final PipelinesProvider pipelinesProvider, S3Client s3Client) {
        super(pipelinesProvider, s3Client);
    }

    @Override
    public void handle(final HttpExchange exchange) throws IOException {
        baseHandle(exchange, false);
    }
}