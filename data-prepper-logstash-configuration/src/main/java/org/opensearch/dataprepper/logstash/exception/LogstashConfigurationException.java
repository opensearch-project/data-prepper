/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.exception;

/**
 * Exception for Logstash configuration converter
 *
 * @since 1.2
 */
public class LogstashConfigurationException extends RuntimeException {

    public LogstashConfigurationException(String errorMessage) {
        super(errorMessage);
    }

    public LogstashConfigurationException(String errorMessage, Exception inner) {
        super(errorMessage, inner);
    }

}
