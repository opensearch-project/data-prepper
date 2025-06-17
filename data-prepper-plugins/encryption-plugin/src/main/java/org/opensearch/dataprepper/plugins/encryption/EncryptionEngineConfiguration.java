/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Duration;

/**
 * An interface available to plugins via the encryption plugin extension which represents encryption engine
 * configuration.
 */
// TODO: avoid using JsonSubTypes and dynamically identify the sub-types.
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
        @JsonSubTypes.Type(value = KmsEncryptionEngineConfiguration.class, name = KmsEncryptionEngineConfiguration.NAME)
})
public interface EncryptionEngineConfiguration {
    /**
     * Represents the type of the encryption engine.
     */
    String name();

    /**
     * Whether encrypted data key rotation is enabled.
     */
    boolean rotationEnabled();

    /**
     * Retrieves the rotation interval.
     */
    Duration getRotationInterval();
}
