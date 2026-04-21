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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;

import java.util.List;

@Getter
public class JoinRelation {

    @NotBlank
    @JsonProperty("parent")
    private String parentTable;

    @NotBlank
    @JsonProperty("child")
    private String childTable;

    @NotEmpty
    @JsonProperty("parent_key")
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    private List<String> parentKey;

    @NotEmpty
    @JsonProperty("child_key")
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    private List<String> childKey;

    @NotEmpty
    @JsonProperty("child_primary_key")
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    private List<String> childPrimaryKey;

    @JsonProperty("join_type")
    private JoinType joinType = JoinType.ONE_TO_MANY;

    @JsonProperty("max_child_records")
    private int maxChildRecords = 1000;
}
