/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.common;

public class RemoteOperationAndService {
    final String remoteService;
    final String remoteOperation;
    public RemoteOperationAndService(final String remoteOperation, final String remoteService) {
        this.remoteOperation = remoteOperation;
        this.remoteService = remoteService;
    }

    public String getOperation() {
        return remoteOperation;
    }

    public String getService() {
        return remoteService;
    }

    public boolean isNull() {
        return remoteOperation == null || remoteService == null;
    }
}

