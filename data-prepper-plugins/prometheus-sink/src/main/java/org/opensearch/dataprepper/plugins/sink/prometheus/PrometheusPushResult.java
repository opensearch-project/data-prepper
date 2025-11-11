/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.prometheus;

public class PrometheusPushResult {
    private final boolean isSuccess;
    private final int statusCode;
    
    public PrometheusPushResult(final boolean isSuccess, final int statusCode) {
        this.isSuccess = isSuccess;
        this.statusCode = statusCode;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public int getStatusCode() {
        return statusCode;
    }
}

