/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model.sourcecoordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * The Data Prepper configuration for source coordination to be used for sources that implement {@link org.opensearch.dataprepper.model.source.RequiresSourceCoordination}
 * @since 2.2
 */

public class SourceCoordinationConfig {

    private final SourceCoordinationStoreConfig sourceCoordinationStoreConfig;

    @JsonCreator
    public SourceCoordinationConfig(@JsonProperty("store") final SourceCoordinationStoreConfig sourceCoordinationStoreConfig) {
        Objects.requireNonNull(sourceCoordinationStoreConfig, "source_coordination store must not be null");
        this.sourceCoordinationStoreConfig = sourceCoordinationStoreConfig;
    }

    public SourceCoordinationStoreConfig getSourceCoordinationStoreConfig() {
        return sourceCoordinationStoreConfig;
    }
}
