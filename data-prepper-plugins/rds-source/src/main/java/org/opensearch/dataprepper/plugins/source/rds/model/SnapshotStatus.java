/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

public enum SnapshotStatus {
    AVAILABLE("available"),
    COPYING("copying"),
    CREATING("creating");

    private final String statusName;

    SnapshotStatus(final String statusName) {
        this.statusName = statusName;
    }

    public String getStatusName() {
        return statusName;
    }
}
