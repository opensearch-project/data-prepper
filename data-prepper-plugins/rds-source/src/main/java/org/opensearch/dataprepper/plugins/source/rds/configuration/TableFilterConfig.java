/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import lombok.Getter;

import java.util.Collections;
import java.util.List;

@Getter
public class TableFilterConfig {

    @JsonProperty("include")
    @Size(max = 1000, message = "Table filter list should not be more than 1000")
    private List<String> include = Collections.emptyList();

    @JsonProperty("exclude")
    @Size(max = 1000, message = "Table filter list should not be more than 1000")
    private List<String> exclude = Collections.emptyList();
}
