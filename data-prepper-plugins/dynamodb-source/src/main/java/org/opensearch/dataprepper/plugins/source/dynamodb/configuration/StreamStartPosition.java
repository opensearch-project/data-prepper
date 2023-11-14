/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;


/**
 * Always default to LATEST.
 * Support of TRIM_HORIZON is taken out,
 * that is due to a concern raised recently about the root shards may be expired while processing.
 */
public enum StreamStartPosition {
    LATEST
}
