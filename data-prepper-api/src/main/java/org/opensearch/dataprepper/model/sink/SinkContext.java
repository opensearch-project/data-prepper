/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import java.util.Collection;

public class SinkContext {
    private final String tagsTargetKey;
    private final Collection<String> routes;

    public SinkContext(final String tagsTargetKey, final Collection<String> routes) {
        this.tagsTargetKey = tagsTargetKey;
        this.routes = routes;
    }
    
    public String getTagsTargetKey() {
        return tagsTargetKey;
    }

    public Collection<String> getRoutes() {
        return routes;
    }
}

