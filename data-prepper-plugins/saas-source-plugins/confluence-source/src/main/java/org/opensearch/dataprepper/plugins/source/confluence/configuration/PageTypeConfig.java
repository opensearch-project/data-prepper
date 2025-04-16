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
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.confluence.utils.ConfluenceContentType;

import java.util.ArrayList;
import java.util.List;

@Getter
public class PageTypeConfig {

    @JsonProperty("include")
    @Size(max = 1000, message = "Page type filter should not be more than 1000")
    List<String> include = new ArrayList<>();

    @JsonProperty("exclude")
    @Size(max = 1000, message = "Page type filter should not be more than 1000")
    List<String> exclude = new ArrayList<>();

    @AssertTrue(message = "Confluence PageType should be one of [page, blogpost, comment, attachment]")
    boolean isValidPageType() {
        return checkGivenListForValidPageTypes(include)
                && checkGivenListForValidPageTypes(exclude)
                && noOverlapBetweenIncludeAndExclude();
    }

    @AssertTrue(message = "There should be no overlap between include and exclude values under PageType filter")
    boolean noOverlapBetweenIncludeAndExclude() {
        return include.stream().noneMatch(exclude::contains);
    }

    boolean checkGivenListForValidPageTypes(List<String> list) {
        for (String value : list) {
            if (ConfluenceContentType.fromString(value) == null) {
                return false;
            }
        }
        return true;
    }
}
