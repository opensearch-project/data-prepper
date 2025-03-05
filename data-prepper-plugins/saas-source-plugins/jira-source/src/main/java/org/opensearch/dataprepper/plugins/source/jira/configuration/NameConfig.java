/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */


package org.opensearch.dataprepper.plugins.source.jira.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Getter
public class NameConfig {

    Pattern projectKeysRegex = Pattern.compile("^[A-Z0-9]+$");

    @JsonProperty("include")
    @Size(max = 100, message = "Project name type filter should not be more than 100")
    List<String> include = new ArrayList<>();

    @JsonProperty("exclude")
    @Size(max = 100, message = "Project name type filter should not be more than 100")
    List<String> exclude = new ArrayList<>();

    @AssertTrue(message = "Jira Project keys should be alphanumeric")
    boolean isValidProjectKeys() {
        return checkGivenListForRegex(include) && checkGivenListForRegex(exclude);
    }

    boolean checkGivenListForRegex(List<String> list) {
        for (String value : list) {
            if (value != null && !projectKeysRegex.matcher(value).matches()) {
                return false;
            }
        }
        return true;
    }
}
