/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class ResyncProgressState {
    @JsonProperty("foreignKeyName")
    private String foreignKeyName;

    @JsonProperty("updatedValue")
    private Object updatedValue;

    @JsonProperty("primaryKeys")
    private List<String> primaryKeys;
}
