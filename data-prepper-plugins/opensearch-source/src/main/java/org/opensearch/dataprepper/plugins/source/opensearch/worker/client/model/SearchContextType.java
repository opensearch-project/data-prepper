/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

public enum SearchContextType {
    SCROLL,
    POINT_IN_TIME,
    NONE
}
