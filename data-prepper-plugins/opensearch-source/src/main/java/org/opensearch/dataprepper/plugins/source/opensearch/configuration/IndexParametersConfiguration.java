/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;

import java.util.Collections;
import java.util.List;

public class IndexParametersConfiguration {

    @JsonProperty("include")
    @Valid
    private List<OpenSearchIndex> include = Collections.emptyList();

    @JsonProperty("exclude")
    @Valid
    private List<OpenSearchIndex> exclude = Collections.emptyList();

    public List<OpenSearchIndex> getIncludedIndices() {
        return include;
    }

    public List<OpenSearchIndex> getExcludedIndices() {
        return exclude;
    }

}
