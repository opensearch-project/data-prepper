/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.util.Arrays;


public class PrepareConnectionTest {

    OpenSearchSourceConfiguration openSearchSourceConfiguration = new OpenSearchSourceConfiguration();

    PrepareConnection prepareConnection = new PrepareConnection();

    @BeforeEach
    void intialSetup() {
        openSearchSourceConfiguration.setHosts(Arrays.asList("http://localhost:9200"));
    }


    @Test
    public void prepareOpensearchConnectionTest() throws MalformedURLException {
        Assert.assertNotNull(prepareConnection.prepareOpensearchConnection(openSearchSourceConfiguration));
    }

    @Test
    public void prepareElasticSearchConnectionTest() throws MalformedURLException {
        Assert.assertNotNull(prepareConnection.prepareElasticSearchConnection(openSearchSourceConfiguration));
    }


}
