package org.opensearch.dataprepper.plugins.source.jira.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;


@Getter
@Setter
public class IssueBean {

    /**
     * -- GETTER --
     * Expand options that include additional issue details in the response.
     *
     * @return expand expand
     */
    @JsonProperty("expand")
    private String expand = null;

    /**
     * -- GETTER --
     * The ID of the issue.
     *
     * @return id id
     */
    @JsonProperty("id")
    private String id = null;

    /**
     * -- GETTER --
     * The URL of the issue details.
     *
     * @return self self
     */
    @JsonProperty("self")
    private String self = null;

    /**
     * -- GETTER --
     * The key of the issue.
     *
     * @return key key
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
