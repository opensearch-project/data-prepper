/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink.opensearch;

import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestHighLevelClient;

import javax.ws.rs.HttpMethod;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class IndexStateManagement {
    public static boolean checkISMEnabled(final RestHighLevelClient restHighLevelClient) throws IOException {
        final ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();
        request.includeDefaults(true);
        final ClusterGetSettingsResponse response = restHighLevelClient.cluster().getSettings(request, RequestOptions.DEFAULT);
        final String enabled = response.getSetting(IndexConstants.ISM_ENABLED_SETTING);
        return enabled != null && enabled.equals("true");
    }

    /**
     * @return ISM policy_id optional that needs to be attached to index settings.
     */
    public static Optional<String> checkAndCreatePolicy(
            final RestHighLevelClient restHighLevelClient, final String indexType) throws IOException {
        if (indexType.equals(IndexConstants.RAW)) {
            // TODO: replace with new _opensearch API
            final String endPoint = "/_opendistro/_ism/policies/" + IndexConstants.RAW_ISM_POLICY;
            Request request = createPolicyRequestFromFile(endPoint, IndexConstants.RAW_ISM_FILE_WITH_ISM_TEMPLATE);
            try {
                restHighLevelClient.getLowLevelClient().performRequest(request);
            } catch (ResponseException e1) {
                final String msg = e1.getMessage();
                if (msg.contains("Invalid field: [ism_template]")) {
                    request = createPolicyRequestFromFile(endPoint, IndexConstants.RAW_ISM_FILE_NO_ISM_TEMPLATE);
                    try {
                        restHighLevelClient.getLowLevelClient().performRequest(request);
                    } catch (ResponseException e2) {
                        if (e2.getMessage().contains("version_conflict_engine_exception")
                                || e2.getMessage().contains("resource_already_exists_exception")) {
                            // Do nothing - likely caused by
                            // (1) a race condition where the resource was created by another host before this host's
                            // restClient made its request;
                            // (2) policy already exists in the cluster
                        } else {
                            throw e2;
                        }
                    }
                    return Optional.of(IndexConstants.RAW_ISM_POLICY);
                } else if (e1.getMessage().contains("version_conflict_engine_exception")
                        || e1.getMessage().contains("resource_already_exists_exception")) {
                    // Do nothing - likely caused by
                    // (1) a race condition where the resource was created by another host before this host's
                    // restClient made its request;
                    // (2) policy already exists in the cluster
                } else {
                    throw e1;
                }
            }
        }
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    public static void attachPolicy(
            final IndexConfiguration configuration, final String ismPolicyId, final String rolloverAlias) {
        configuration.getIndexTemplate().putIfAbsent("settings", new HashMap<>());
        if (ismPolicyId != null) {
            ((Map<String, Object>) configuration.getIndexTemplate().get("settings")).put(IndexConstants.ISM_POLICY_ID_SETTING, ismPolicyId);
        }
        ((Map<String, Object>) configuration.getIndexTemplate().get("settings")).put(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING, rolloverAlias);
    }

    private static Request createPolicyRequestFromFile(final String endPoint, final String fileName) throws IOException {
        final StringBuilder policyJsonBuffer = new StringBuilder();
        try (final InputStream inputStream = IndexStateManagement.class.getClassLoader().getResourceAsStream(fileName);
             final BufferedReader reader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(inputStream)))) {
            reader.lines().forEach(line -> policyJsonBuffer.append(line).append("\n"));
        }
        final Request request = new Request(HttpMethod.PUT, endPoint);
        request.setJsonEntity(policyJsonBuffer.toString());
        return request;
    }
}
