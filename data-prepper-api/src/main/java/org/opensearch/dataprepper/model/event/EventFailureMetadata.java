/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

public interface EventFailureMetadata {
    EventFailureMetadata with(String key, Object value);
}

