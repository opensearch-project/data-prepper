/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */


package org.opensearch.dataprepper.plugins.source.confluence.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.List;

/**
 * The result of a CQL search.
 */
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfluenceSearchResults {


    @JsonProperty("start")
    private Integer start = null;

    @JsonProperty("limit")
    private Integer limit = null;

    @JsonProperty("size")
    private Integer size = null;

    @JsonProperty("results")
    private List<ConfluenceItem> results = null;

}
