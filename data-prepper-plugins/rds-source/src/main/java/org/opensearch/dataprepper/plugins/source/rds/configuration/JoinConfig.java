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
import jakarta.validation.Valid;
import lombok.Getter;

import java.util.Collections;
import java.util.List;

@Getter
public class JoinConfig {

    @Valid
    @JsonProperty("relations")
    private List<JoinRelation> relations = Collections.emptyList();

    @JsonProperty("version_field")
    private String versionField = "__versions";
}
