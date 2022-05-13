/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch.bulk;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Represents JSON which is already serialized for use in the {@link PreSerializedJsonpMapper}.
 */
public interface SerializedJson extends SizedDocument {

    byte[] getSerializedJson();

    /**
     * Creates a new {@link SerializedJson} from a JSON string.
     *
     * @param jsonString The serialized JSON string which forms this JSON data.
     * @return A new {@link SerializedJson}.
     */
    static SerializedJson fromString(String jsonString) {
        Objects.requireNonNull(jsonString);
        return new SerializedJsonImpl(jsonString.getBytes(StandardCharsets.UTF_8));
    }
}
