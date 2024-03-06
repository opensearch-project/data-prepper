/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.exception;

public class InvalidIPAddressException extends EnrichFailedException {
    public InvalidIPAddressException(final String exceptionMsg) {
        super(exceptionMsg);
    }
}
