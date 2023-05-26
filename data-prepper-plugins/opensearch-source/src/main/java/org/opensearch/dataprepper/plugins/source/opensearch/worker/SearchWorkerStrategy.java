/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;

/**
 * Search Worker Strategy determines which SearchWorker (PITSearchWorker or ScrollSearchWorker) based on the
 * {@link OpenSearchSourceConfiguration}.
 *
 * @since 2.4
 */
public class SearchWorkerStrategy {

    /**
     * Get the SearchWorker based on the based on the {@link OpenSearchSourceConfiguration}.
     * @param openSearchSourceConfiguration the plugins configuration
     * @return a runnable to execute a strategy for polling the source cluster
     * @since 2.4
     */
    public Runnable getSearchWorker(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        // todo: to implement
        return null;
    }
}
