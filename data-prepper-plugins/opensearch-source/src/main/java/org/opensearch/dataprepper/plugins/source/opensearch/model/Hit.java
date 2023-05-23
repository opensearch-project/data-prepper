/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.model;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "_index",
    "_id",
    "_score",
    "_source",
    "sort"
})
public class Hit {

    @JsonProperty("_index")
    private String index;
    @JsonProperty("_id")
    private String id;
    @JsonProperty("_score")
    private Object score;
    @JsonProperty("_source")
    private Source source;
    @JsonProperty("sort")
    private List<Integer> sort;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("_index")
    public String getIndex() {
        return index;
    }

    @JsonProperty("_index")
    public void setIndex(String index) {
        this.index = index;
    }

    @JsonProperty("_id")
    public String getId() {
        return id;
    }

    @JsonProperty("_id")
    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty("_score")
    public Object getScore() {
        return score;
    }

    @JsonProperty("_score")
    public void setScore(Object score) {
        this.score = score;
    }

    @JsonProperty("_source")
    public Source getSource() {
        return source;
    }

    @JsonProperty("_source")
    public void setSource(Source source) {
        this.source = source;
    }

    @JsonProperty("sort")
    public List<Integer> getSort() {
        return sort;
    }

    @JsonProperty("sort")
    public void setSort(List<Integer> sort) {
        this.sort = sort;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}
