/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.exception;

/**
 * Implementation class for DownloadFailedException Custom exception
 */
public class DownloadFailedException extends EngineFailureException {
    public DownloadFailedException(final String exceptionMsg) {
        super(exceptionMsg);
    }

    public DownloadFailedException(final String exceptionMsg, Throwable cause){
        super(exceptionMsg, cause);
    }

}
