/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.opensearch.dataprepper.model.source.SourceCoordinator;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.parser.model.sourcecoordination.DynamoDBSourceCoordinationStoreConfig;
import org.opensearch.dataprepper.parser.model.sourcecoordination.SourceCoordinationConfig;
import org.opensearch.dataprepper.parser.model.sourcecoordination.SourceCoordinationStoreConfig;

import java.util.Objects;

/**
 * A factory class that will create the {@link org.opensearch.dataprepper.model.source.SourceCoordinator} implementation based on the
 * source_coordination configuration
 * @since 2.2
 */
public class SourceCoordinatorFactory {

    public SourceCoordinatorFactory() {

    }

    public SourceCoordinator provideSourceCoordinator(final DataPrepperConfiguration dataPrepperConfiguration) {
        final SourceCoordinationConfig sourceCoordinationConfig = dataPrepperConfiguration.getSourceCoordinationConfig();

        if (Objects.isNull(sourceCoordinationConfig)) {
            return null;
        }

        final SourceCoordinationStoreConfig sourceCoordinationStoreConfig = sourceCoordinationConfig.getSourceCoordinationStoreConfig();

        if (Objects.isNull(sourceCoordinationStoreConfig)) {
            return null;
        }

        if (sourceCoordinationStoreConfig instanceof DynamoDBSourceCoordinationStoreConfig) {
            return new DynamoDbSourceCoordinator(sourceCoordinationConfig);
        }

        return null;
    }
}
