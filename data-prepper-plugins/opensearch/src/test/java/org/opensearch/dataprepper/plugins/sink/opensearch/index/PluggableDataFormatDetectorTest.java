/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.indices.AliasDefinition;
import org.opensearch.client.opensearch.indices.ExistsAliasRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.GetAliasRequest;
import org.opensearch.client.opensearch.indices.GetAliasResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.opensearch.indices.get_alias.IndexAliases;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.TransportOptions;
import org.opensearch.client.transport.endpoints.BooleanResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.ComponentTemplateAPIWrapperTest.toJsonData;

@ExtendWith(MockitoExtension.class)
class PluggableDataFormatDetectorTest {
    private static final String ALIAS = "otel-v1-apm-span";
    private static final String WRITE_INDEX = "otel-v1-apm-span-000002";

    @Mock
    private OpenSearchClient openSearchClient;
    @Mock
    private OpenSearchIndicesClient openSearchIndicesClient;
    @Mock
    private OpenSearchTransport openSearchTransport;
    @Mock
    private TransportOptions openSearchTransportOptions;

    private Map<String, String> responses;
    private List<String> requests;

    @BeforeEach
    void setUp() throws IOException {
        responses = new HashMap<>();
        requests = new ArrayList<>();
        lenient().when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
        lenient().when(openSearchClient._transport()).thenReturn(openSearchTransport);
        lenient().when(openSearchClient._transportOptions()).thenReturn(openSearchTransportOptions);
        lenient().when(openSearchTransport.performRequest(isNull(), any(), eq(openSearchTransportOptions))).thenAnswer(invocation -> {
            final Endpoint<Void, JsonData, ErrorResponse> endpoint = invocation.getArgument(1);
            final String url = endpoint.requestUrl(null);
            assertThat(endpoint.method(null), equalTo("GET"));
            requests.add(url + " " + new TreeMap<>(endpoint.queryParameters(null)));
            return toJsonData(responses.get(url));
        });
    }

    private PluggableDataFormatDetector createObjectUnderTest() {
        return new PluggableDataFormatDetector(openSearchClient);
    }

    private void givenNoExistingIndex() throws IOException {
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(false));
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(false));
    }

    private void givenAlias(final Map<String, Boolean> isWriteIndexByIndex) throws IOException {
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(true));
        final GetAliasResponse.Builder response = new GetAliasResponse.Builder();
        isWriteIndexByIndex.forEach((index, isWriteIndex) -> response.putResult(index,
                IndexAliases.of(aliases -> aliases.aliases(ALIAS, AliasDefinition.of(alias -> alias.isWriteIndex(isWriteIndex))))));
        when(openSearchIndicesClient.getAlias(any(GetAliasRequest.class))).thenReturn(response.build());
    }

    private void givenIndexSettings(final String index, final String settings, final String defaults) {
        responses.put("/" + index + "/_settings", "{\"" + index + "\":{\"settings\":{" + settings + "},\"defaults\":{" + defaults + "}}}");
    }

    private void givenClusterSettings(final String persistent, final String defaults) {
        responses.put("/_cluster/settings", "{\"persistent\":{" + persistent + "},\"transient\":{},\"defaults\":{" + defaults + "}}");
    }

    private static String parquetClusterDefaults() {
        return "\"opensearch.experimental.feature.pluggable.dataformat.enabled\":\"true\","
                + "\"cluster.pluggable.dataformat.enabled\":\"true\","
                + "\"cluster.pluggable.dataformat\":\"composite\","
                + "\"cluster.composite.primary_data_format\":\"parquet\","
                + "\"cluster.pluggable.dataformat.restrict.allowlist\":[\".tasks\",\".kibana\"]";
    }

    @ParameterizedTest
    @CsvSource({
            "true, composite, parquet, true",
            "true, parquet, , true",
            "true, composite, lucene, false",
            "true, composite, , false",
            "true, '', parquet, false",
            "true, lucene, parquet, false",
            "false, composite, parquet, false",
            ", composite, parquet, false"
    })
    void isParquetPrimary_resolves_settings(final String enabled, final String format, final String primary, final boolean expected) {
        assertThat(PluggableDataFormatDetector.isParquetPrimary(enabled, format, primary), equalTo(expected));
    }

    @Test
    void returns_false_for_cluster_without_pluggable_data_formats() throws IOException {
        givenClusterSettings("", "\"cluster.pluggable.dataformat.enabled\":\"false\",\"cluster.pluggable.dataformat\":\"\","
                + "\"opensearch.experimental.feature.pluggable.dataformat.enabled\":\"false\"");

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(false));
    }

    @Test
    void returns_false_for_cluster_without_any_pluggable_settings() throws IOException {
        givenNoExistingIndex();
        givenClusterSettings("", "\"plugins.index_state_management.enabled\":\"true\"");

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(false));
    }

    @Test
    void returns_true_when_new_indices_default_to_parquet_primary() throws IOException {
        givenNoExistingIndex();
        givenClusterSettings("", parquetClusterDefaults());

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(true));
        assertThat(requests, contains("/_cluster/settings {flat_settings=true, include_defaults=true}"));
    }

    @Test
    void returns_false_when_persistent_setting_overrides_primary_data_format() throws IOException {
        givenNoExistingIndex();
        givenClusterSettings("\"cluster.composite.primary_data_format\":\"lucene\"", parquetClusterDefaults());

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(false));
    }

    @Test
    void returns_false_when_feature_flag_is_disabled() throws IOException {
        givenClusterSettings("", parquetClusterDefaults()
                .replace("\"opensearch.experimental.feature.pluggable.dataformat.enabled\":\"true\"",
                        "\"opensearch.experimental.feature.pluggable.dataformat.enabled\":\"false\""));

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(false));
    }

    @Test
    void returns_false_when_alias_matches_allowlist_prefix() throws IOException {
        givenNoExistingIndex();
        givenClusterSettings("", parquetClusterDefaults().replace("\".kibana\"", "\"otel-v1-\""));

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(false));
    }

    @Test
    void returns_false_when_first_index_matches_allowlist_prefix_longer_than_alias() throws IOException {
        givenNoExistingIndex();
        givenClusterSettings("", parquetClusterDefaults().replace("\".kibana\"", "\"otel-v1-apm-span-0\""));

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(false));
    }

    @Test
    void reads_settings_of_write_index_when_alias_exists() throws IOException {
        givenClusterSettings("", parquetClusterDefaults());
        givenAlias(Map.of("otel-v1-apm-span-000001", false, WRITE_INDEX, true));
        givenIndexSettings(WRITE_INDEX, "\"index.pluggable.dataformat.enabled\":\"true\",\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\"", "");

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(true));
        assertThat(requests, contains("/_cluster/settings {flat_settings=true, include_defaults=true}",
                "/" + WRITE_INDEX + "/_settings {flat_settings=true, include_defaults=true}"));
    }

    @Test
    void returns_false_without_reading_index_settings_when_feature_flag_is_disabled() throws IOException {
        givenClusterSettings("", "\"opensearch.experimental.feature.pluggable.dataformat.enabled\":\"false\"");

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(false));
        assertThat(requests, contains("/_cluster/settings {flat_settings=true, include_defaults=true}"));
        verify(openSearchIndicesClient, never()).existsAlias(any(ExistsAliasRequest.class));
    }

    @Test
    void returns_false_when_existing_write_index_is_not_pluggable() throws IOException {
        givenClusterSettings("", parquetClusterDefaults());
        givenAlias(Map.of(WRITE_INDEX, true));
        givenIndexSettings(WRITE_INDEX, "", "\"index.pluggable.dataformat.enabled\":\"false\",\"index.pluggable.dataformat\":\"\"");

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(false));
        assertThat(requests.size(), equalTo(2));
    }

    @Test
    void uses_only_index_of_alias_without_write_index_flag() throws IOException {
        givenClusterSettings("", parquetClusterDefaults());
        givenAlias(Collections.singletonMap(WRITE_INDEX, null));
        givenIndexSettings(WRITE_INDEX, "\"index.pluggable.dataformat.enabled\":\"true\",\"index.pluggable.dataformat\":\"parquet\"", "");

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(true));
    }

    @Test
    void falls_back_to_cluster_settings_when_alias_has_no_write_index() throws IOException {
        givenClusterSettings("", parquetClusterDefaults());
        final Map<String, Boolean> indices = new HashMap<>();
        indices.put("otel-v1-apm-span-000001", null);
        indices.put(WRITE_INDEX, false);
        givenAlias(indices);

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(true));
        assertThat(requests, contains("/_cluster/settings {flat_settings=true, include_defaults=true}"));
    }

    @Test
    void reads_settings_of_concrete_index_named_like_alias() throws IOException {
        givenClusterSettings("", parquetClusterDefaults());
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(false));
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(true));
        responses.put("/" + ALIAS + "/_settings", "{\"" + ALIAS + "\":{\"settings\":{},\"defaults\":{"
                + "\"index.pluggable.dataformat.enabled\":\"true\",\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\"}}}");

        assertThat(createObjectUnderTest().isParquetPrimary(ALIAS), equalTo(true));
    }
}
