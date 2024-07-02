/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public enum ExportStatus {
    CANCELED,
    CANCELING,
    COMPLETE,
    FAILED,
    IN_PROGRESS,
    STARTING;

    private static final Map<String, ExportStatus> TYPES_MAP = Arrays.stream(ExportStatus.values())
            .collect(Collectors.toMap(
                    Enum::name,
                    value -> value
            ));
    private static final Set<ExportStatus> TERMINAL_STATUSES = Set.of(CANCELED, COMPLETE, FAILED);

    public static ExportStatus fromString(final String name) {
        return TYPES_MAP.get(name);
    }

    public static boolean isTerminal(final String name) {
        ExportStatus status = fromString(name);
        return status != null && TERMINAL_STATUSES.contains(status);
    }
}