/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Which signal wins when deriving a service's environment.
 */
public enum EnvironmentSource {
    /** Today's behaviour: the AWS platform ({@code eks:default}, {@code ec2:default}, ...) first, then
     * {@code deployment.environment.name} / {@code deployment.environment}. */
    PLATFORM_FIRST("platform_first"),
    /** {@code deployment.environment.name} / {@code deployment.environment} first; the platform-derived
     * value is the fallback. Changes the environment (and so the service-map node identity and the RED
     * metric labels) of services that set both. */
    DEPLOYMENT_FIRST("deployment_first");

    private final String value;

    EnvironmentSource(final String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static EnvironmentSource fromValue(final String value) {
        for (final EnvironmentSource source : values()) {
            if (source.value.equalsIgnoreCase(value)) {
                return source;
            }
        }
        throw new IllegalArgumentException("Unknown environment_source: " + value
                + " (expected platform_first or deployment_first)");
    }
}
