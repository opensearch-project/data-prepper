package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import java.time.Duration;

/**
 * Marker interface to all the SAAS connectors source server configuration metadata
 */
public interface SourceServerMetadata {

    /**
     * Returns the polling timezone offset
     *
     * @return the polling timezone offset
     */
    Duration getPollingTimezoneOffset();
}
