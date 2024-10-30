/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator.enhanced;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

/**
 * <p>
 * A base definition of a {@link EnhancedPartition} in the coordination store.
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
public abstract class EnhancedSourcePartition<T> implements EnhancedPartition<T> {

    private static final Logger LOG = LoggerFactory.getLogger(EnhancedSourcePartition.class);
    private static final ObjectMapper objectMapper = new ObjectMapper(new JsonFactory())
            .registerModule(new JavaTimeModule());

    private SourcePartitionStoreItem sourcePartitionStoreItem;

    public SourcePartitionStoreItem getSourcePartitionStoreItem() {
        return sourcePartitionStoreItem;
    }

    public void setSourcePartitionStoreItem(SourcePartitionStoreItem sourcePartitionStoreItem) {
        this.sourcePartitionStoreItem = sourcePartitionStoreItem;
    }

    /**
     * Helper method to convert progress state.
     * This is because the state is currently stored as a String in the coordination store.
     *
     * @param progressStateClass               class of progress state
     * @param serializedPartitionProgressState serialized value of the partition progress state
     * @return returns the converted value of the progress state
     */
    public T convertStringToPartitionProgressState(Class<T> progressStateClass, final String serializedPartitionProgressState) {
        if (Objects.isNull(serializedPartitionProgressState)) {
            return null;
        }

        try {
            if (progressStateClass != null) {
                return objectMapper.readValue(serializedPartitionProgressState, progressStateClass);
            }
            return objectMapper.readValue(serializedPartitionProgressState, new TypeReference<>() {
            });
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to convert string to partition progress state class {}: ", progressStateClass != null ? progressStateClass.getName() : null, e);
            return null;
        }
    }

    /**
     * Helper method to convert progress state to String
     * This is because the state is currently stored as a String in the coordination store.
     *
     * @param partitionProgressState optional parameter indicating the partition progress state
     * @return returns the progress state as string
     */
    public String convertPartitionProgressStatetoString(Optional<T> partitionProgressState) {
        if (partitionProgressState.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(partitionProgressState.get());
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to convert partition progress state class to string: ", e);
            return null;
        }
    }
}
