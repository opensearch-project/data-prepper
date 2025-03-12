/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.model.plugin;

/**
 * Exception thrown when a secret could not be updated.
 *
 * @since 2.11
 */
public class FailedToUpdatePluginConfigValueException extends RuntimeException {

    public FailedToUpdatePluginConfigValueException(final String message) {
        super(message);
    }

    public FailedToUpdatePluginConfigValueException(final String message, Throwable e) {
        super(message, e);
    }
}
