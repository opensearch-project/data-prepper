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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;


@Setter
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfluenceItem {

    /**
     * The ID of the issue.
     */
    @JsonProperty("id")
    private String id = null;

    /**
     * The type of the issue.
     */
    @JsonProperty("type")
    private String type = null;

    /**
     * The type of the issue.
     */
    @JsonProperty("status")
    private String status = null;

    /**
     * The type of the issue.
     */
    @JsonProperty("title")
    private String title = null;

    /**
     * Space this content belongs to
     */
    @JsonProperty("space")
    private SpaceItem spaceItem;

    @JsonProperty("history")
    private ContentHistory history;

    @JsonIgnore
    public long getCreatedTimeMillis() {
        if (history == null) {
            return 0L;
        }
        return history.getCreatedDateInMillis();
    }

    @JsonIgnore
    public long getUpdatedTimeMillis() {
        if (history == null) {
            return 0L;
        }
        return history.getLastUpdatedInMillis();
    }

}
