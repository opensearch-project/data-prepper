/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.exception;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SaaSCrawlerExceptionTest {
    
    @Test
    void constructor_WithMessageAndRetryableFlag_SetsMessageAndFlag() {
        // Given
        String errorMessage = "Test error message";
        boolean retryable = true;

        // When
        SaaSCrawlerException exception = new SaaSCrawlerException(errorMessage, retryable);

        // Then
        assertEquals(errorMessage, exception.getMessage());
        assertTrue(exception.isRetryable());
    }

    @Test
    void constructor_WithMessageAndRetryableFlagFalse_SetsMessageAndFlag() {
        // Given
        String errorMessage = "Test error message";
        boolean retryable = false;

        // When
        SaaSCrawlerException exception = new SaaSCrawlerException(errorMessage, retryable);

        // Then
        assertEquals(errorMessage, exception.getMessage());
        assertFalse(exception.isRetryable());
    }

    @Test
    void constructor_WithMessageCauseAndRetryableFlag_SetsAllFields() {
        // Given
        String errorMessage = "Test error message";
        Throwable cause = new IllegalArgumentException("Test cause");
        boolean retryable = true;

        // When
        SaaSCrawlerException exception = new SaaSCrawlerException(errorMessage, cause, retryable);

        // Then
        assertEquals(errorMessage, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertTrue(exception.isRetryable());
    }

    @Test
    void constructor_WithMessageCauseAndRetryableFlagFalse_SetsAllFields() {
        // Given
        String errorMessage = "Test error message";
        Throwable cause = new IllegalArgumentException("Test cause");
        boolean retryable = false;

        // When
        SaaSCrawlerException exception = new SaaSCrawlerException(errorMessage, cause, retryable);

        // Then
        assertEquals(errorMessage, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertFalse(exception.isRetryable());
    }

}
