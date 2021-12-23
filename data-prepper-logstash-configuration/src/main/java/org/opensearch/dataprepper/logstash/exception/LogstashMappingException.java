/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.exception;

/**
 * Exception thrown when attempting to map between Logstash configuration model and Data Prepper model
 *
 * @since 1.2
 */
public class LogstashMappingException extends LogstashConfigurationException{

    public LogstashMappingException(String errorMessage) {
        super(errorMessage);
    }

    public LogstashMappingException(String errorMessage, Exception inner) {
        super(errorMessage, inner);
    }

}
