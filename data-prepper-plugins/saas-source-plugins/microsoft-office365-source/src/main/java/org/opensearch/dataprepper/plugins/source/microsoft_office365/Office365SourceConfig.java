/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.hibernate.validator.constraints.time.DurationMax;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.AuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;

import java.time.Duration;

/**
 * Configuration class for Office 365 source plugin.
 */
@Getter
public class Office365SourceConfig implements CrawlerSourceConfig {
    private static final int NUMBER_OF_WORKERS = 4;

    /**
     * The Office 365 tenant ID that uniquely identifies the Microsoft Entra organization.
     */
    @NotNull
    @JsonProperty("tenant_id")
    private String tenantId;

    /**
     * Authentication configuration for accessing Office 365 APIs.
     * Contains OAuth2 credentials including client ID and client secret for now
     */
    @NotNull
    @JsonProperty("authentication")
    @Valid
    private AuthenticationConfiguration authenticationConfiguration;

    /**
     * Flag to enable/disable acknowledgments for processed records.
     * When enabled, ensures records are processed at least once.
     */
    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    /**
     * Time range for lookback data collection using ISO 8601 duration format.
     * Specifies how far back in time to collect data from the current time.
     * Default: null (no lookback, only incremental data)
     * Maximum: P7D (7 days due to Office 365 API limitation)
     */
    @JsonProperty("range")
    @DurationMax(days = 7, message = "Range cannot exceed 7 days due to Office 365 API limitation")
    private Duration range;

    /**
     * Gets the look back range as minutes for the crawler framework.
     * This method supports minute-level granularity for historical pulls.
     *
     * @return the number of minutes to look back, or 0 if no range is specified
     */
    public long getLookBackMinutes() {
        if (range == null || range.isZero() || range.isNegative()) {
            return 0;
        }
        return range.toMinutes();
    }

    /**
     * Gets the look back range as hours for compatibility with existing crawler framework.
     *
     * @return the number of hours to look back, or 0 if no range is specified
     * @deprecated Use {@link #getLookBackMinutes()} for minute-level granularity support
     */
    @Deprecated
    public int getLookBackHours() {
        if (range == null || range.toHours() <= 0) {
            return 0;
        }
        return (int) range.toHours();
    }

    @Override
    public int getNumberOfWorkers() {
        return NUMBER_OF_WORKERS;
    }

    @Override
    public boolean isAcknowledgments() {
        return acknowledgments;
    }
}
