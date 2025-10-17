package org.opensearch.dataprepper.plugins.source.source_crawler.base;

/**
 * Marker interface to all the SAAS connectors configuration
 */
public interface CrawlerSourceConfig {

    int DEFAULT_NUMBER_OF_WORKERS = 1;

    /**
     * Number of worker threads enabled for this source
     *
     * @return Integer indicating number of worker threads
     */
    int getNumberOfWorkers();

    /**
     * Boolean to indicate if acknowledgments enabled for this source
     *
     * @return boolean indicating acknowledgement state
     */
    boolean isAcknowledgments();
}
