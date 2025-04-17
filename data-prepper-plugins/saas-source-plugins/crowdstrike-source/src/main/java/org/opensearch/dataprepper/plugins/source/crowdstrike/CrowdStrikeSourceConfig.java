package org.opensearch.dataprepper.plugins.source.crowdstrike;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.crowdstrike.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;

/**
 * Configuration class for the CrowdStrike source plugin.
 */
@Getter
public class CrowdStrikeSourceConfig implements CrawlerSourceConfig {
    /**
     * Batch size for fetching Threat Intel Feeds
     */
    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static final int DEFAULT_NUMBER_OF_WORKERS = 5;

    @JsonProperty("authentication")
    @Valid
    protected AuthenticationConfig authenticationConfig;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    @JsonProperty("workers")
    @Min(1)
    @Max(50)
    @Valid
    private int numWorkers = DEFAULT_NUMBER_OF_WORKERS;

    @Override
    public int getBatchSize() {
        return DEFAULT_BATCH_SIZE;
    }

}
