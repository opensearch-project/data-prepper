/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class OpenSearchIndex {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchIndex.class);

    @JsonProperty("index_name_regex")
    @NotBlank
    private String indexNameRegex;

    @JsonIgnore
    private Pattern indexNamePattern;

    public Pattern getIndexNamePattern() {
        return indexNamePattern;
    }

    @AssertTrue(message = "The value of index_name_regex is not a valid regex pattern")
    boolean isRegexValid() {
        try {
            indexNamePattern = Pattern.compile(indexNameRegex);
            return true;
        } catch (final PatternSyntaxException e){
            LOG.error("Invalid index_name_regex pattern '{}': {}", indexNameRegex, e.getMessage());
        }
        return false;
    }
}
