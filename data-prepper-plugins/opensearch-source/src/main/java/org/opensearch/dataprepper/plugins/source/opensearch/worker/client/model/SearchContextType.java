/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum SearchContextType {
    SCROLL("scroll"),
    POINT_IN_TIME("point_in_time"),
    NONE("none");

    private final String searchContextType;

    SearchContextType(final String searchContextType) {
        this.searchContextType = searchContextType;
    }

    @JsonValue
    public String getSearchContextType() {
        return searchContextType;
    }

}
