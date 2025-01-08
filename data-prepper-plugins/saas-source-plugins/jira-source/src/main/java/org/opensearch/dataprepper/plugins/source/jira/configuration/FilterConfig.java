/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class FilterConfig {
    @JsonProperty("project")
    private ProjectConfig  projectConfig;

    @JsonProperty("status")
    private StatusConfig statusConfig;

    @JsonProperty("issue_type")
    private IssueTypeConfig issueTypeConfig;
}
