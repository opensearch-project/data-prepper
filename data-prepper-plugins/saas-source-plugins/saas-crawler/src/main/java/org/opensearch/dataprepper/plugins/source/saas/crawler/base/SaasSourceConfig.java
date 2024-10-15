package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import java.time.Duration;

/**
 * Marker interface to all the SAAS connectors configuration
 */
public interface SaasSourceConfig {

    public final int DEFAULT_NUMBER_OF_WORKERS = 1;
    public static final Duration DEFAULT_BACKOFF_MILLIS = Duration.ofMillis(30000);

    public boolean isValid();
}
