/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.meter;

import io.micrometer.core.instrument.step.StepRegistryConfig;

import java.time.Duration;

public interface EMFLoggingRegistryConfig extends StepRegistryConfig {
    EMFLoggingRegistryConfig DEFAULT = k -> null;

    default boolean highResolution() {
        return step().compareTo(Duration.ofMinutes(1)) < 0;
    }

    @Override
    default String prefix() {
        return "emf-logging";
    }
}
