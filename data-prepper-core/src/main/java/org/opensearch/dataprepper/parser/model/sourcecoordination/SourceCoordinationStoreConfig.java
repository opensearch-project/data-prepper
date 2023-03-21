/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model.sourcecoordination;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * The interface to be implemented to support different database store configurations
 * for {@link org.opensearch.dataprepper.model.source.SourceCoordinator} implementations
 * @since 2.2
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({ @JsonSubTypes.Type(DynamoDBSourceCoordinationStoreConfig.class)})
public interface SourceCoordinationStoreConfig {
    String getStoreName();
    Object getStoreSettings();
}
