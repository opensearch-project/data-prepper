/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public class IndexParametersConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(IndexParametersConfiguration.class);

    @JsonProperty("include")
    @Valid
    private List<IncludedIndex> include = Collections.emptyList();

    @JsonProperty("exclude")
    private List<String> exclude = Collections.emptyList();

    @JsonIgnore
    private List<Pattern> excludePatterns = Collections.emptyList();

    public List<IncludedIndex> getIncludedIndices() {
        return include;
    }

    public List<Pattern> getExcludePatterns() {
        return excludePatterns;
    }

    @AssertTrue(message = "one of the exclude regex patterns is invalid")
    boolean isExcludeRegexValid() {
        try {
            excludePatterns = exclude.stream().map(Pattern::compile).collect(Collectors.toList());
            return true;
        } catch (final PatternSyntaxException e){
            LOG.error("Invalid exclude regex pattern: ", e);
        }
        return false;
    }

}
