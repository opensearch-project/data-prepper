/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.MainResponse;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
@EnabledIfSystemProperty(named = "tests.mtls.client.cert", matches = ".+")
@EnabledIfSystemProperty(named = "tests.mtls.client.key", matches = ".+")
class OpenSearchClientCertIT {

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private static String getHost() {
        return System.getProperty("tests.opensearch.host", "localhost:9200");
    }

    private static String getUser() {
        return System.getProperty("tests.opensearch.user", "admin");
    }

    private static String getPassword() {
        return System.getProperty("tests.opensearch.password", "admin");
    }

    private static String getClientCert() {
        return System.getProperty("tests.mtls.client.cert");
    }

    private static String getClientKey() {
        return System.getProperty("tests.mtls.client.key");
    }

    @Test
    void client_with_mtls_can_connect_and_get_cluster_info() throws IOException {
        final ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://" + getHost()))
                .withInsecure(true)
                .withUsername(getUser())
                .withPassword(getPassword())
                .withClientCert(getClientCert())
                .withClientKey(getClientKey())
                .build();

        try (RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier)) {
            assertThat(client, notNullValue());

            final MainResponse response = client.info(RequestOptions.DEFAULT);
            assertThat(response, notNullValue());
            assertThat(response.getClusterName(), notNullValue());
        }
    }

    @Test
    void client_with_mtls_can_create_index() throws IOException {
        final ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration.Builder(
                Collections.singletonList("https://" + getHost()))
                .withInsecure(true)
                .withUsername(getUser())
                .withPassword(getPassword())
                .withClientCert(getClientCert())
                .withClientKey(getClientKey())
                .build();

        try (RestHighLevelClient client = connectionConfiguration.createClient(awsCredentialsSupplier)) {
            final CreateIndexRequest createIndexRequest = new CreateIndexRequest("mtls-integration-test-index");
            final CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            assertThat(createIndexResponse.isAcknowledged(), equalTo(true));
        }
    }
}
