/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */


package org.opensearch.dataprepper.plugins.source.jira.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.List;

/**
 * The result of a JQL search.
 */
@Getter
public class SearchResults {
    @JsonProperty("expand")
    private String expand = null;

    @JsonProperty("startAt")
    private Integer startAt = null;

    @JsonProperty("maxResults")
    private Integer maxResults = null;

    @JsonProperty("total")
    private Integer total = null;

    @JsonProperty("issues")
    private List<IssueBean> issues = null;

}
