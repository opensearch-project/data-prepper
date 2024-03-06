/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.exception;

public class NoValidDatabaseFoundException extends EngineFailureException {
    public NoValidDatabaseFoundException(final String exceptionMsg) {
        super(exceptionMsg);
    }
}
