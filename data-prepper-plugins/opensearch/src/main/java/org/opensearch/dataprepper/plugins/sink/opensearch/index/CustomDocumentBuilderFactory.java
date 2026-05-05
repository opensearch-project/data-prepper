/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

public final class CustomDocumentBuilderFactory {

    public CustomDocumentBuilder create(final IndexType indexType) {
        if (indexType == IndexType.TSDB) {
            return new TSDBDocumentBuilder();
        }
        return null;
    }
}