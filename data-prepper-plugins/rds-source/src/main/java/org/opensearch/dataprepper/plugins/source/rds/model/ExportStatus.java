/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.util.Set;

public enum ExportStatus {
    CANCELED,
    CANCELING,
    COMPLETE,
    FAILED,
    IN_PROGRESS,
    STARTING;

    public static final Set<String> TERMINAL_STATUS_NAMES = Set.of(CANCELED.name(), COMPLETE.name(), FAILED.name());
}