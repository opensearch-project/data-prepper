/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.exception;

/**
 * Implementation class for DownloadFailedException Custom exception
 */
public class DownloadFailedException extends RuntimeException {
    public DownloadFailedException(final String exceptionMsg) {
        super(exceptionMsg);
    }
}
