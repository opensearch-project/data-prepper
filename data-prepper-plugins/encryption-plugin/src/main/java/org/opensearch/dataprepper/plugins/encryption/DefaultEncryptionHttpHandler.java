/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import com.sun.net.httpserver.HttpExchange;
import org.opensearch.dataprepper.model.encryption.EncryptionHttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Set;

public class DefaultEncryptionHttpHandler implements EncryptionHttpHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultEncryptionHttpHandler.class);

    private final Set<EncryptionRotationHandler> encryptionRotationHandlers;

    public static DefaultEncryptionHttpHandler create(final Set<EncryptionRotationHandler> encryptionRotationHandlers) {
        return new DefaultEncryptionHttpHandler(encryptionRotationHandlers);
    }

    private DefaultEncryptionHttpHandler(final Set<EncryptionRotationHandler> encryptionRotationHandlers) {
        this.encryptionRotationHandlers = encryptionRotationHandlers;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        final String requestMethod = exchange.getRequestMethod();
        if (!requestMethod.equals(HttpMethod.POST)) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, 0);
            exchange.getResponseBody().close();
            return;
        }

        final String path = exchange.getRequestURI().getPath();
        try {
            for (final EncryptionRotationHandler encryptionRotationHandler : encryptionRotationHandlers) {
                final String encryptionId = encryptionRotationHandler.getEncryptionId();
                if (!path.endsWith(encryptionId)) {
                    continue;
                }

                try {
                    LOG.info("Rotating encryptionId: {}.", encryptionId);
                    encryptionRotationHandler.handleRotation();
                    LOG.info("Finished rotating encryptionId: {}.", encryptionId);
                } catch (Exception e) {
                    LOG.error("Caught exception rotating encryptionId: {}.",
                            encryptionRotationHandler.getEncryptionId(), e);
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
                }
            }

            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
        } finally {
            exchange.getResponseBody().close();
        }
    }
}
