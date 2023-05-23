/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import java.net.URL;
import org.opensearch.client.opensearch.OpenSearchClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;

/**
 * used for creating connection
 */
public class OpenSearchClientBuilder {

    public OpenSearchClient createOpenSearchClient(final URL url){
        return null;
    }

    public ElasticsearchClient createElasticSearchCLient(final URL url) {
        return null;
    }
}
