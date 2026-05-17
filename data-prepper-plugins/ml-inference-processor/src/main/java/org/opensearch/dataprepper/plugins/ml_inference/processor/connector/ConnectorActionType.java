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

/**
 * Action types a connector can perform against a remote ML service.
 * Mirrors the ActionType concept in ml-commons connectors.
 */
public enum ConnectorActionType {
    PREDICT,
    BATCH_PREDICT,
    BATCH_PREDICT_STATUS,
    CANCEL_BATCH_PREDICT
}
