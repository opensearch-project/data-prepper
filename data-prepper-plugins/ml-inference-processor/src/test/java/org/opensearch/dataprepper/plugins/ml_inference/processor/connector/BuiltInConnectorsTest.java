/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.connector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BuiltInConnectorsTest {

    static Stream<String> builtInModelIds() {
        return BuiltInConnectors.listBuiltInModelIds().stream();
    }

    @Test
    void listBuiltInModelIds_returnsNonEmptyList() {
        final List<String> modelIds = BuiltInConnectors.listBuiltInModelIds();
        assertThat(modelIds.size(), greaterThan(0));
    }

    @Test
    void findConnectorJson_whenModelIdIsNull_returnsEmpty() {
        assertThat(BuiltInConnectors.findConnectorJson(null), is(Optional.empty()));
    }

    @Test
    void findConnectorJson_whenModelIdIsUnknown_returnsEmpty() {
        assertThat(BuiltInConnectors.findConnectorJson("unknown.model-id"), is(Optional.empty()));
    }

    @ParameterizedTest
    @MethodSource("builtInModelIds")
    void findConnectorJson_allBuiltInModels_returnValidAwsSigv4Connector(final String modelId) {
        final String json = BuiltInConnectors.findConnectorJson(modelId).orElseThrow();

        assertThat(json, containsString("\"protocol\""));
        assertThat(json, containsString("aws_sigv4"));
        assertThat(json, containsString(modelId));
        assertThat(json, containsString("BATCH_PREDICT"));

        final AbstractConnector connector = assertDoesNotThrow(() -> AbstractConnector.fromJson(json));
        assertThat(connector.getProtocol(), is("aws_sigv4"));
        assertTrue(connector instanceof AwsConnector);
    }

    @ParameterizedTest
    @MethodSource("builtInModelIds")
    void findConnectorJson_allBuiltInModels_connectorHasRequiredActions(final String modelId) {
        final String json = BuiltInConnectors.findConnectorJson(modelId).orElseThrow();
        final AbstractConnector connector = assertDoesNotThrow(() -> AbstractConnector.fromJson(json));

        assertTrue(connector.findAction("PREDICT").isPresent());
        assertTrue(connector.findAction("BATCH_PREDICT").isPresent());
        assertTrue(connector.findAction("BATCH_PREDICT_STATUS").isPresent());
        assertTrue(connector.findAction("CANCEL_BATCH_PREDICT").isPresent());
    }

    @ParameterizedTest
    @MethodSource("builtInModelIds")
    void findConnectorJson_allBuiltInModels_connectorParametersIncludeServiceNameAndModel(final String modelId) {
        final String json = BuiltInConnectors.findConnectorJson(modelId).orElseThrow();
        final AbstractConnector connector = assertDoesNotThrow(() -> AbstractConnector.fromJson(json));

        assertThat(connector.getParameters().get("service_name"), is("bedrock"));
        assertThat(connector.getParameters().get("model"), is(not(Optional.empty())));
    }
}
