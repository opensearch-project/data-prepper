package org.opensearch.dataprepper.plugins.source.jira.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;


@Getter
@Setter
public class IssueBean {

    /**
     * Expand options that include additional issue details in the response.
     */
    @JsonProperty("expand")
    private String expand = null;

    /**
     * The ID of the issue.
     */
    @JsonProperty("id")
    private String id = null;

    /**
     * The URL of the issue details.
     */
    @JsonProperty("self")
    private String self = null;

    /**
     * The key of the issue.
     */
    @JsonProperty("key")
    private String key = null;

    @JsonProperty("renderedFields")
    private Map<String, Object> renderedFields = null;

    @JsonProperty("properties")
    private Map<String, Object> properties = null;

    @JsonProperty("names")
    private Map<String, String> names = null;

    @JsonProperty("fields")
    private Map<String, Object> fields = null;

}
