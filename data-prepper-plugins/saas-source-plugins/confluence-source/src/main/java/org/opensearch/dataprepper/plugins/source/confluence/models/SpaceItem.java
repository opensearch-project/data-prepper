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
public class SpaceItem {

    /**
     * The ID of the issue.
     */
    @JsonProperty("id")
    private int id;

    /**
     * The type of the issue.
     */
    @JsonProperty("key")
    private String key = null;

    /**
     * The type of the issue.
     */
    @JsonProperty("alias")
    private String alias = null;

    /**
     * The type of the issue.
     */
    @JsonProperty("name")
    private String name = null;

    /**
     * The type of the issue.
     */
    @JsonProperty("status")
    private String status = null;

    /**
     * The type of the issue.
     */
    @JsonProperty("type")
    private String type = null;


}
