/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum JiraContentType {
    PROJECT("PROJECT"),
    ISSUE("ISSUE"),
    COMMENT("COMMENT"),
    ATTACHMENT("ATTACHMENT"),
    WORKLOG("WORKLOG");

    @Getter
    private final String type;
}
