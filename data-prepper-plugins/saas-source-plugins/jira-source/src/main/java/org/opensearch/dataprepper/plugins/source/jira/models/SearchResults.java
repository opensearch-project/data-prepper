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
