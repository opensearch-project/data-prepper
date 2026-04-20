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
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BuiltInConnectorsTest {

    @Test
    void findConnectorJson_whenModelIdIsNull_returnsEmpty() {
        assertThat(BuiltInConnectors.findConnectorJson(null), is(Optional.empty()));
    }

    @Test
    void findConnectorJson_whenModelIdIsUnknown_returnsEmpty() {
        assertThat(BuiltInConnectors.findConnectorJson("unknown.model-id"), is(Optional.empty()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            BuiltInConnectors.TITAN_EMBED_V2_MODEL_ID,
            BuiltInConnectors.TITAN_MULTIMODAL_EMBED_V1_MODEL_ID
    })
    void findConnectorJson_whenModelIdIsKnown_returnsNonEmpty(final String modelId) {
        final Optional<String> result = BuiltInConnectors.findConnectorJson(modelId);
        assertTrue(result.isPresent());
    }

    @Test
    void findConnectorJson_titanEmbedV2_returnsValidJson() throws Exception {
        final String json = BuiltInConnectors.findConnectorJson(BuiltInConnectors.TITAN_EMBED_V2_MODEL_ID).orElseThrow();

        assertThat(json, containsString("\"protocol\""));
        assertThat(json, containsString("aws_sigv4"));
        assertThat(json, containsString("amazon.titan-embed-text-v2:0"));
        assertThat(json, containsString("BATCH_PREDICT"));

        // Must deserialize into the correct subclass without error
        final AbstractConnector connector = assertDoesNotThrow(() -> AbstractConnector.fromJson(json));
        assertThat(connector.getProtocol(), is("aws_sigv4"));
        assertTrue(connector instanceof AwsConnector);
    }

    @Test
    void findConnectorJson_titanMultimodalV1_returnsValidJson() throws Exception {
        final String json = BuiltInConnectors.findConnectorJson(BuiltInConnectors.TITAN_MULTIMODAL_EMBED_V1_MODEL_ID).orElseThrow();

        assertThat(json, containsString("\"protocol\""));
        assertThat(json, containsString("aws_sigv4"));
        assertThat(json, containsString("amazon.titan-embed-image-v1"));
        assertThat(json, containsString("BATCH_PREDICT"));

        final AbstractConnector connector = assertDoesNotThrow(() -> AbstractConnector.fromJson(json));
        assertThat(connector.getProtocol(), is("aws_sigv4"));
        assertTrue(connector instanceof AwsConnector);
    }

    @Test
    void findConnectorJson_titanEmbedV2_connectorHasExpectedActions() throws Exception {
        final String json = BuiltInConnectors.findConnectorJson(BuiltInConnectors.TITAN_EMBED_V2_MODEL_ID).orElseThrow();
        final AbstractConnector connector = AbstractConnector.fromJson(json);

        assertTrue(connector.findAction("PREDICT").isPresent());
        assertTrue(connector.findAction("BATCH_PREDICT").isPresent());
        assertTrue(connector.findAction("BATCH_PREDICT_STATUS").isPresent());
        assertTrue(connector.findAction("CANCEL_BATCH_PREDICT").isPresent());
    }

    @Test
    void findConnectorJson_titanMultimodalV1_connectorHasExpectedActions() throws Exception {
        final String json = BuiltInConnectors.findConnectorJson(BuiltInConnectors.TITAN_MULTIMODAL_EMBED_V1_MODEL_ID).orElseThrow();
        final AbstractConnector connector = AbstractConnector.fromJson(json);

        assertTrue(connector.findAction("PREDICT").isPresent());
        assertTrue(connector.findAction("BATCH_PREDICT").isPresent());
        assertTrue(connector.findAction("BATCH_PREDICT_STATUS").isPresent());
        assertTrue(connector.findAction("CANCEL_BATCH_PREDICT").isPresent());
    }

    @Test
    void findConnectorJson_titanEmbedV2_connectorParametersIncludeDefaults() throws Exception {
        final String json = BuiltInConnectors.findConnectorJson(BuiltInConnectors.TITAN_EMBED_V2_MODEL_ID).orElseThrow();
        final AbstractConnector connector = AbstractConnector.fromJson(json);

        assertThat(connector.getParameters().get("service_name"), is("bedrock"));
        assertThat(connector.getParameters().get("model"), is("amazon.titan-embed-text-v2:0"));
        assertThat(connector.getParameters().get("dimensions"), is("1024"));
        assertThat(connector.getParameters().get("normalize"), is("true"));
    }

    @Test
    void findConnectorJson_titanMultimodalV1_connectorParametersIncludeDefaults() throws Exception {
        final String json = BuiltInConnectors.findConnectorJson(BuiltInConnectors.TITAN_MULTIMODAL_EMBED_V1_MODEL_ID).orElseThrow();
        final AbstractConnector connector = AbstractConnector.fromJson(json);

        assertThat(connector.getParameters().get("service_name"), is("bedrock"));
        assertThat(connector.getParameters().get("model"), is("amazon.titan-embed-image-v1"));
    }
}
