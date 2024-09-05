/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http.codec;

import com.linecorp.armeria.common.HttpData;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Codec parses the content of HTTP request into custom Java type.
 * <p>
 */
public interface Codec<T> {
    /**
     * parse the request into custom type
     *
     * @param httpData The content of the original HTTP request
     * @return The target data type
     */
    T parse(HttpData httpData) throws IOException;

    /**
     * Serializes parsed data back into a UTF-8 string.
     * <p>
     * This API will split into multiple bodies based on splitLength. Note that if a single
     * item is larger than this, it will be output and exceed that length.
     *
     * @param parsedData The parsed data
     * @param serializedBodyConsumer A {@link Consumer} to accept each serialized body
     * @param splitLength The length at which to split serialized bodies.
     * @throws IOException A failure writing data.
     */
    void serialize(final T parsedData,
                   final Consumer<String> serializedBodyConsumer,
                   final int splitLength) throws IOException;


    /**
     * Serializes parsed data back into a UTF-8 string.
     * <p>
     * This API will not split the data into chunks.
     *
     * @param parsedData The parsed data
     * @param serializedBodyConsumer A {@link Consumer} to accept the serialized body
     * @throws IOException A failure writing data.
     */
    default void serialize(final T parsedData, final Consumer<String> serializedBodyConsumer) throws IOException {
        serialize(parsedData, serializedBodyConsumer, Integer.MAX_VALUE);
    }
}
