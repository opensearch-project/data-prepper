/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.coordination;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

/**
 * <p>
 * A base definition of a {@link Partition} in the coordination store.
 * All partitions must extend this.
 * </p>
 * We store the {SourcePartitionStoreItem} in the partition.
 * The benefits are:
 * <ul>
 *     <li>Don't have to query again before each updates</li>
 *     <li>Can perform Optimistic locking on updates.</li>
 * </ul>
 * Future improvement may be made for this. As we don't have access to Version Number.
 *
 * @param <T> The progress state class
 */
public abstract class SourcePartition<T> implements Partition<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultEnhancedSourceCoordinator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SourcePartitionStoreItem sourcePartitionStoreItem;

    protected SourcePartitionStoreItem getSourcePartitionStoreItem() {
        return sourcePartitionStoreItem;
    }


    protected void setSourcePartitionStoreItem(SourcePartitionStoreItem sourcePartitionStoreItem) {
        this.sourcePartitionStoreItem = sourcePartitionStoreItem;
    }


    /**
     * Helper method to convert progress state.
     * This is because the state is currently stored as a String in the coordination store.
     */
    protected Optional<T> convertStringToPartitionProgressState(Class<T> T, final String serializedPartitionProgressState) {
        if (Objects.isNull(serializedPartitionProgressState)) {
            return Optional.empty();
        }

        try {
            return Optional.of(MAPPER.readValue(serializedPartitionProgressState, T));
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to convert string to partition progress state class {}: {}", T.getName(), e);
            return Optional.empty();
        }
    }

    /**
     * Helper method to convert progress state to map (for global state)
     * This is because the state is currently stored as a String in the coordination store.
     */
    protected Optional<T> convertStringToMap(final String serializedPartitionProgressState) {
        if (Objects.isNull(serializedPartitionProgressState)) {
            return Optional.empty();
        }

        try {
            return Optional.of(MAPPER.readValue(serializedPartitionProgressState, new TypeReference<>() {
            }));
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to convert string to map: {}", e);
            return Optional.empty();
        }
    }

    /**
     * Helper method to convert progress state to String
     * This is because the state is currently stored as a String in the coordination store.
     */
    protected String convertPartitionProgressStatetoString(Optional<T> partitionProgressState) {
        if (partitionProgressState.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(partitionProgressState.get());
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to convert partition progress state class to string: {}", e);
            return null;
        }
    }
}
