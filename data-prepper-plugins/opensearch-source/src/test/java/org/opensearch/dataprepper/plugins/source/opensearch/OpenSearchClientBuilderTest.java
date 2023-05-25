/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;

public class OpenSearchClientBuilderTest {
    OpenSearchClientBuilder clientBuilder = new OpenSearchClientBuilder();
    @Test
    public void createOpenSearchClientTest() throws MalformedURLException {
        Assert.assertNotNull(clientBuilder.createOpenSearchClient(new URL("http://localhost:9200")));
    }
    @Test
    public void createElasticSearchCLientTest() throws MalformedURLException {
        Assert.assertNotNull(clientBuilder.createElasticSearchClient(new URL("http://localhost:9200")));
    }
}
