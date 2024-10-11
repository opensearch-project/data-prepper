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
     * Validates the content of the HTTP request.
     *
     * @param content The content of the original HTTP request
     * @throws IOException A failure validating data.
     */
    void validate(HttpData content) throws IOException;

    /*
     * Serializes the HttpData and split into multiple bodies based on splitLength.
     * <p>
     * The serialized bodies are passed to the serializedBodyConsumer.
     * <p>
     * This API will split into multiple bodies based on splitLength. Note that if a single
     * item is larger than this, it will be output and exceed that length.
     *
     * @param content The content of the original HTTP request
     * @param serializedBodyConsumer A {@link Consumer} to accept each serialized body
     * @param splitLength The length at which to split serialized bodies.
     * @throws IOException A failure writing data.
     */
    void serializeSplit(HttpData content, Consumer<String> serializedBodyConsumer, int splitLength) throws IOException;
}
