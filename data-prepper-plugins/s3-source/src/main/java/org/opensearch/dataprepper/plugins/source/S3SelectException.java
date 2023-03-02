/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import java.io.Serializable;

/**
 * This exception is thrown when S3 select process fail.
 */
public class S3SelectException extends RuntimeException implements Serializable {
    
    S3SelectException(final String message){
        super(message);
    }
}
