/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.office365;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.office365.auth.Office365AuthenticationProvider;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import javax.inject.Named;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.office365.utils.Constants.CONTENT_TYPES;
import static org.opensearch.dataprepper.plugins.source.office365.utils.Constants.MANAGEMENT_API_BASE_URL;

/**
 * REST client for interacting with Office 365 Management API.
 * Handles all HTTP communications with the Office 365 endpoints.
 */
@Slf4j
@Named
public class Office365RestClient {
    private static final String AUDIT_LOG_FETCH_LATENCY = "auditLogFetchLatency";
    private static final String SEARCH_CALL_LATENCY = "searchCallLatency";
    private static final String AUDIT_LOGS_REQUESTED = "auditLogsRequested";
    private static final String AUDIT_LOG_REQUESTS_FAILED = "auditLogRequestsFailed";
    private static final String AUDIT_LOG_REQUESTS_SUCCESS = "auditLogRequestsSuccess";
    private static final String SEARCH_REQUESTS_FAILED = "searchRequestsFailed";

    private final RestTemplate restTemplate = new RestTemplate();
    private final Office365AuthenticationProvider authConfig;
    private final Timer auditLogFetchLatencyTimer;
    private final Timer searchCallLatencyTimer;
    private final Counter auditLogsRequestedCounter;
    private final Counter auditLogRequestsFailedCounter;
    private final Counter auditLogRequestsSuccessCounter;
    private final Counter searchRequestsFailedCounter;

    public Office365RestClient(final Office365AuthenticationProvider authConfig,
                               final PluginMetrics pluginMetrics) {
        // TODO: Abstract into a Office365PluginMetrics
        this.authConfig = authConfig;
        this.auditLogFetchLatencyTimer = pluginMetrics.timer(AUDIT_LOG_FETCH_LATENCY);
        this.searchCallLatencyTimer = pluginMetrics.timer(SEARCH_CALL_LATENCY);
        this.auditLogsRequestedCounter = pluginMetrics.counter(AUDIT_LOGS_REQUESTED);
        this.auditLogRequestsFailedCounter = pluginMetrics.counter(AUDIT_LOG_REQUESTS_FAILED);
        this.auditLogRequestsSuccessCounter = pluginMetrics.counter(AUDIT_LOG_REQUESTS_SUCCESS);
        this.searchRequestsFailedCounter = pluginMetrics.counter(SEARCH_REQUESTS_FAILED);
    }

    /**
     * Starts and verifies subscriptions for Office 365 audit logs.
     */
    public void startSubscriptions() {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setBearerAuth(authConfig.getAccessToken());
            headers.setContentType(MediaType.APPLICATION_JSON);

            // Check current subscriptions
            final String SUBSCRIPTION_LIST_URL = MANAGEMENT_API_BASE_URL + "%s/activity/feed/subscriptions/list";
            String listUrl = String.format(SUBSCRIPTION_LIST_URL, authConfig.getTenantId());

            ResponseEntity<String> listResponse = restTemplate.exchange(
                    listUrl,
                    HttpMethod.GET,
                    new HttpEntity<>(headers),
                    String.class
            );
            log.debug("Current subscriptions: {}", listResponse.getBody());

            // Start subscriptions for each content type
            headers.setContentLength(0);
            for (String contentType : CONTENT_TYPES) {
                final String SUBSCRIPTION_START_URL = MANAGEMENT_API_BASE_URL + "%s/activity/feed/subscriptions/start?contentType=%s";
                String url = String.format(SUBSCRIPTION_START_URL,
                        authConfig.getTenantId(),
                        contentType);

                try {
                    ResponseEntity<String> response = restTemplate.exchange(
                            url,
                            HttpMethod.POST,
                            new HttpEntity<>(headers),
                            String.class
                    );
                    log.debug("Started subscription for {}: {}", contentType, response.getBody());
                } catch (HttpClientErrorException e) {
                    if (e.getResponseBodyAsString().contains("AF20024")) {
                        log.debug("Subscription for {} is already enabled", contentType);
                    } else {
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error in subscription process", e);
            throw new RuntimeException("Failed to initialize subscriptions: " + e.getMessage(), e);
        }
    }

    /**
     * Searches for audit logs of a specific content type within a time range.
     */
    public List<Map<String, Object>> searchAuditLogs(final String contentType,
                                                     final Instant startTime,
                                                     final Instant endTime) {
        final String GET_AUDIT_LOGS_URL = MANAGEMENT_API_BASE_URL +
                "%s/activity/feed/subscriptions/content?contentType=%s&startTime=%s&endTime=%s";

        final String url = String.format(GET_AUDIT_LOGS_URL,
                authConfig.getTenantId(),
                contentType,
                startTime.toString(),
                endTime.toString());

        final HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(authConfig.getAccessToken());

        return searchCallLatencyTimer.record(() -> {
            try {
                ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
                        url,
                        HttpMethod.GET,
                        new HttpEntity<>(headers),
                        new ParameterizedTypeReference<>() {}
                );
                return response.getBody();
            } catch (Exception e) {
                log.error("Error while fetching audit logs for content type {}", contentType, e);
                searchRequestsFailedCounter.increment();
                throw new RuntimeException("Failed to fetch audit logs", e);
            }
        });
    }

    /**
     * Retrieves a specific audit log entry by its content ID.
     */
    public String getAuditLog(final String contentId) {
        auditLogsRequestedCounter.increment();

        final String FETCH_AUDIT_LOG_URL = MANAGEMENT_API_BASE_URL + "%s/activity/feed/audit/%s";

        final String url = String.format(FETCH_AUDIT_LOG_URL,
                authConfig.getTenantId(),
                contentId);

        final HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(authConfig.getAccessToken());

        return auditLogFetchLatencyTimer.record(() -> {
            try {
                ResponseEntity<String> response = restTemplate.exchange(
                        url,
                        HttpMethod.GET,
                        new HttpEntity<>(headers),
                        String.class
                );
                auditLogRequestsSuccessCounter.increment();
                return response.getBody();
            } catch (Exception e) {
                log.error("Error while fetching audit log with ID {}", contentId, e);
                auditLogRequestsFailedCounter.increment();
                throw new RuntimeException("Failed to fetch audit log", e);
            }
        });
    }
}
