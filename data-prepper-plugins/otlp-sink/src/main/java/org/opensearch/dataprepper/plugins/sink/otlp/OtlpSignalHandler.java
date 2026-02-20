/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import software.amazon.awssdk.utils.Pair;

import java.util.List;

/**
 * Interface for handling signal-specific OTLP operations.
 * Implementations provide encoding and request building for specific signal types (traces, metrics, logs).
 */
public interface OtlpSignalHandler {
    /**
     * Encodes an event into its protobuf representation.
     *
     * @param event the event to encode
     * @return the encoded protobuf object
     * @throws Exception if encoding fails
     */
    Object encodeEvent(Event event) throws Exception;

    /**
     * Gets the serialized size of the encoded data.
     *
     * @param encodedData the encoded protobuf object
     * @return the size in bytes
     */
    long getSerializedSize(Object encodedData);

    /**
     * Builds the OTLP export request payload from a batch of encoded events.
     *
     * @param batch the batch of encoded events with their handles
     * @return the serialized request payload
     */
    byte[] buildRequestPayload(List<Pair<Object, EventHandle>> batch);

    /**
     * Parses the response and extracts partial success information.
     *
     * @param responseBytes the response bytes
     * @return a pair of (rejectedCount, errorMessage)
     * @throws Exception if parsing fails
     */
    Pair<Long, String> parsePartialSuccess(byte[] responseBytes) throws Exception;
}
