package org.opensearch.dataprepper.plugins.source.source_crawler.base;

/**
 * Marker interface to all the SAAS connectors configuration
 */
public interface CrawlerSourceConfig {

    int DEFAULT_NUMBER_OF_WORKERS = 1;

    int getBatchSize();

    /**
     * Boolean to indicate if acknowledgments enabled for this source
     *
     * @return boolean indicating acknowledgement state
     */
    boolean isAcknowledgments();

    /**
     * Returns the polling timezone offset in seconds.
     *
     * @return the polling timezone offset in seconds.
     */
    int getPollingTimezoneOffsetInSeconds();
}
