/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class StatusConfig {
    @JsonProperty("include")
    @Size(max = 1000, message = "status type filter should not be more than 1000")
    private List<String> include = new ArrayList<>();

    @JsonProperty("exclude")
    @Size(max = 1000, message = "status type filter should not be more than 1000")
    private List<String> exclude = new ArrayList<>();
}
