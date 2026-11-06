/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.codec;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;

/**
 * Provides {@link JsonFactory} instances configured with Data Prepper's stream read constraints.
 * <p>
 * Events may hold individual string values larger than Jackson's default maximum string length
 * of 20 MB. Any code which parses JSON into events, or which re-reads JSON that Data Prepper
 * itself wrote (such as codecs reading from a buffer), should create its {@link JsonFactory}
 * from this class so that the same maximum string length applies consistently.
 *
 * @since 2.17
 */
public final class JsonFactories {
    /**
     * The maximum JSON string length Data Prepper supports in an event: 64 MB.
     */
    public static final int MAX_JSON_STRING_LENGTH = 64 * 1024 * 1024;

    private JsonFactories() {
    }

    /**
     * Creates a {@link JsonFactory} with the default Data Prepper maximum string length.
     *
     * @return a new {@link JsonFactory}
     * @since 2.17
     */
    public static JsonFactory defaultJsonFactory() {
        return jsonFactory(MAX_JSON_STRING_LENGTH);
    }

    /**
     * Creates a {@link JsonFactory} with the provided maximum string length.
     *
     * @param maxStringLength the maximum JSON string length the factory's parsers permit
     * @return a new {@link JsonFactory}
     * @since 2.17
     */
    public static JsonFactory jsonFactory(final int maxStringLength) {
        return JsonFactory.builder()
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxStringLength(maxStringLength)
                        .build())
                .build();
    }
}
