/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

/**
 * Represents an OpenSearch document with a serialized document size.
 */
public interface SizedDocument {
    /**
     * The size of this document.
     *
     * @return The document size in bytes
     */
    long getDocumentSize();
}
