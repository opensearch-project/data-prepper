/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.time.Duration;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

/**
 * Generic authentication interface for SaaS source plugins.
 * Provides common authentication functionality including credential initialization and renewal.
 */
public interface AuthenticationInterface {
    Logger log = LoggerFactory.getLogger(AuthenticationInterface.class);

    /**
     * Gets the retry interval for credential initialization.
     * This method can be overridden by implementing classes to customize the retry interval.
     * @return the retry interval (default is 10 minutes)
     */
    default Duration getRetryInterval() {
        return Duration.ofMinutes(10);
    }

    /**
     * Initializes credentials with retry logic.
     * This method will attempt to renew credentials and retry on failure.
     */
    default void initCredentials() {
        log.info("Initializing credentials.");
        while (!isCredentialsInitialized()) {
            try {
                renewCredentials();
                setCredentialsInitialized(true);
                log.info("Credentials initialized successfully");
                break;
            } catch (HttpClientErrorException | HttpServerErrorException | SecurityException ex) {
                log.error(NOISY, "Failed to initialize credentials, retrying in {} min. Reason for failure: {}", getRetryInterval().toMinutes(), ex.getMessage());
            } catch (Exception ex) {
                log.error(NOISY, "Failed to initialize credentials due to unexpected error, retrying in {} min. Reason for failure:", getRetryInterval().toMinutes(), ex);
            }
            // Sleep and retry
            try {
                Thread.sleep(getRetryInterval().toMillis());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Credential initialization interrupted", ie);
            }
        }
    }

    /**
     * Renews the authentication credentials.
     * This method should be implemented by concrete authentication providers.
     */
    void renewCredentials();

    /**
     * Checks if credentials have been successfully initialized.
     * This method should be implemented by concrete authentication providers.
     * @return true if credentials are initialized, false otherwise
     */
    boolean isCredentialsInitialized();

    /**
     * Sets the credentials initialization status.
     * This method should be implemented by concrete authentication providers.
     * @param initialized true if credentials are initialized, false otherwise
     */
    void setCredentialsInitialized(boolean initialized);
}
