/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.atlassian.AtlassianSourceConfig;
import org.opensearch.dataprepper.plugins.source.confluence.configuration.FilterConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;

@Getter
public class ConfluenceSourceConfig extends AtlassianSourceConfig implements CrawlerSourceConfig {

    /**
     * Filter Config to filter what tickets get ingested
     */
    @JsonProperty("filter")
    private FilterConfig filterConfig;

    /**
     * Boolean property indicating end to end acknowledgments state
     */
    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    @Override
    public String getOauth2UrlContext() {
        return "confluence";
    }

    /**
     * boolean flag to control whether to preserve the original formatting or not
     */
    @JsonProperty("preserve_formatting")
    private boolean preserveContentFormatting;
}
