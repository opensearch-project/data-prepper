/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions;

public class IndexNotFoundException extends RuntimeException {
    public IndexNotFoundException(final String message) { super (message); }
}
