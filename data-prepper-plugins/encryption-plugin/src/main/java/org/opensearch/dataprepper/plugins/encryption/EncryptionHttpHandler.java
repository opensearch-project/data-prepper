/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import com.sun.net.httpserver.HttpHandler;

/**
 * An interface available to plugins via the encryption plugin extension which supplies the http handler for encryption update.
 */
public interface EncryptionHttpHandler extends HttpHandler {
}
