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
import lombok.Setter;


@Setter
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfluencePaginationLinks {

    /**
     * base url
     */
    @JsonProperty("base")
    private String base = null;

    /**
     * url context
     */
    @JsonProperty("context")
    private String context = null;

    /**
     * next page link if available
     */
    @JsonProperty("next")
    private String next = null;

    /**
     * previous page link if available
     */
    @JsonProperty("previous")
    private String previous = null;

    /**
     * current page link
     */
    @JsonProperty("self")
    private String self = null;

}
