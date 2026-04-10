/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;

@Getter
public class JoinRelation {

    @NotBlank
    @JsonProperty("parent")
    private String parentTable;

    @NotBlank
    @JsonProperty("child")
    private String childTable;

    @NotBlank
    @JsonProperty("parent_key")
    private String parentKey;

    @NotBlank
    @JsonProperty("child_key")
    private String childKey;

    @NotBlank
    @JsonProperty("child_primary_key")
    private String childPrimaryKey;
}
