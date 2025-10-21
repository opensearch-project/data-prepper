/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365.models;

import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class AuditLogsResponse {
    private final List<Map<String, Object>> items;
    private final String nextPageUri;

    public AuditLogsResponse(List<Map<String, Object>> items, String nextPageUri) {
        this.items = items;
        this.nextPageUri = nextPageUri;
    }
}