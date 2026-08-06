/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationInterface;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.SaaSCrawlerException;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;
import org.opensearch.dataprepper.plugins.source.source_crawler.metrics.VendorAPIMetricsRecorder;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry.RetryHandler;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry.DefaultRetryStrategy;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry.DefaultStatusCodeHandler;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

import javax.inject.Named;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.source.microsoft_office365.utils.Constants.CONTENT_TYPES;

/**
 * REST client for interacting with Office 365 Management API.
 * Handles all HTTP communications with the Office 365 endpoints.
 */
@Slf4j
@Named
public class Office365RestClient {
    private static final String MANAGEMENT_API_BASE_URL = "https://manage.office.com/api/v1.0/";
    private static final String SUBSCRIPTION_LIST_URL = MANAGEMENT_API_BASE_URL + "%s/activity/feed/subscriptions/list";
    private static final String SUBSCRIPTION_START_URL = MANAGEMENT_API_BASE_URL + "%s/activity/feed/subscriptions/start?contentType=%s";
    private static final String GET_AUDIT_LOGS_URL = MANAGEMENT_API_BASE_URL + "%s/activity/feed/subscriptions/content?contentType=%s&startTime=%s&endTime=%s";
    
    private final RestTemplate restTemplate = new RestTemplate();
    private final RetryHandler retryHandler;
    private final Office365AuthenticationInterface authConfig;
    private final VendorAPIMetricsRecorder metricsRecorder;

    public Office365RestClient(final Office365AuthenticationInterface authConfig,
                               final VendorAPIMetricsRecorder metricsRecorder) {
        this.authConfig = authConfig;
        this.metricsRecorder = metricsRecorder;
        this.retryHandler = new RetryHandler(
                new DefaultRetryStrategy(),
                new DefaultStatusCodeHandler());
    }

    /**
     * Lists current subscriptions for Office 365 audit logs.
     * 
     * @return List of subscription maps containing contentType, status, and webhook information
     * @throws SaaSCrawlerException if the operation fails
     */
    private List<Map<String, Object>> listSubscriptions() {
        log.info("Listing Office 365 subscriptions");
        String listUrl = String.format(SUBSCRIPTION_LIST_URL, authConfig.getTenantId());

        return metricsRecorder.recordListSubscriptionLatency(() -> {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            try {
                List<Map<String, Object>> result = retryHandler.executeWithRetry(() -> {
                    headers.setBearerAuth(authConfig.getAccessToken());
                    metricsRecorder.recordListSubscriptionCall();
                    
                    ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
                            listUrl,
                            HttpMethod.GET,
                            new HttpEntity<>(headers),
                            new ParameterizedTypeReference<>() {}
                    );
                    log.debug("Current subscriptions: {}", response.getBody());
                    return response.getBody();
                }, authConfig::renewCredentials, metricsRecorder::recordListSubscriptionFailure);
                
                metricsRecorder.recordListSubscriptionSuccess();
                return result;
            } catch (SaaSCrawlerException e) {
                metricsRecorder.recordError(e);
                log.error(NOISY, "Failed to list subscriptions: {}", e.getMessage());
                throw e;
            } catch (Exception e) {
                metricsRecorder.recordError(e);
                log.error(NOISY, "Failed to list subscriptions: {}", e.getMessage());
                throw new SaaSCrawlerException("Failed to list subscriptions: " + e.getMessage(), e, true);
            }
        });
    }

    /**
     * Starts subscriptions for the specified content types.
     * 
     * @param contentTypesToStart List of content types to start subscriptions for
     */
    private void startSubscriptionsForContentTypes(List<String> contentTypesToStart) {
        log.info("Starting {} subscription(s)", contentTypesToStart.size());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setContentLength(0);

        for (String contentType : contentTypesToStart) {
            String url = String.format(SUBSCRIPTION_START_URL,
                    authConfig.getTenantId(),
                    contentType);

            try {
                retryHandler.executeWithRetry(() -> {
                    headers.setBearerAuth(authConfig.getAccessToken());
                    metricsRecorder.recordSubscriptionCall();
                    
                    ResponseEntity<String> response = restTemplate.exchange(
                            url,
                            HttpMethod.POST,
                            new HttpEntity<>(headers),
                            String.class
                    );
                    log.info("Successfully started subscription for {}: {}", contentType, response.getBody());
                    return response;
                }, authConfig::renewCredentials, metricsRecorder::recordSubscriptionFailure);
            } catch (SaaSCrawlerException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RestClientResponseException) {
                    RestClientResponseException restEx = (RestClientResponseException) cause;
                    if (restEx.getResponseBodyAsString().contains("AF20024")) {
                        log.debug("Subscription for {} is already enabled", contentType);
                    } else {
                        metricsRecorder.recordError(e);
                        throw e;
                    }
                } else {
                    metricsRecorder.recordError(e);
                    throw e;
                }
            } catch (Exception e) {
                metricsRecorder.recordError(e);
                throw new SaaSCrawlerException("Failed to start subscription for " + contentType + ": " + e.getMessage(), e, true);
            }
        }
        
        log.info("Successfully started {} subscription(s)", contentTypesToStart.size());
    }

    /**
     * Starts and verifies subscriptions for Office 365 audit logs.
     * Only starts subscriptions for content types that are not already enabled.
     * If listing subscriptions fails, falls back to starting all content types.
     */
    public void startSubscriptions() {
        log.info("Starting Office 365 subscriptions for audit logs");
        
        metricsRecorder.recordSubscriptionLatency(() -> {
            try {
                List<String> contentTypesToStart = new ArrayList<>();

                // Try to get current subscriptions to determine which need to be started
                try {
                    List<Map<String, Object>> currentSubscriptions = listSubscriptions();
                    
                    // Determine which content types are already enabled
                    Set<String> enabledContentTypes = new HashSet<>();
                    for (Map<String, Object> subscription : currentSubscriptions) {
                        String contentType = (String) subscription.get("contentType");
                        String status = (String) subscription.get("status");

                        if ("enabled".equalsIgnoreCase(status)) {
                            enabledContentTypes.add(contentType);
                            log.info("Content type {} is already enabled", contentType);
                        }
                    }

                    // Identify content types that need to be started
                    for (String contentType : CONTENT_TYPES) {
                        if (!enabledContentTypes.contains(contentType)) {
                            contentTypesToStart.add(contentType);
                            log.info("Content type {} needs to be started", contentType);
                        }
                    }

                    // If all content types are already enabled, we're done
                    if (contentTypesToStart.isEmpty()) {
                        log.info("All content types are already enabled. No subscriptions need to be started.");
                        metricsRecorder.recordSubscriptionSuccess();
                        return null;
                    }
                } catch (Exception e) {
                    // If listing subscriptions fails, fall back to starting all content types
                    log.warn("Failed to list subscriptions, will attempt to start all content types as fallback: {}", e.getMessage());
                    contentTypesToStart.clear();
                    for (String contentType : CONTENT_TYPES) {
                        contentTypesToStart.add(contentType);
                    }
                }

                // Start subscriptions for the identified content types
                startSubscriptionsForContentTypes(contentTypesToStart);
                metricsRecorder.recordSubscriptionSuccess();
                return null;
            } catch (SaaSCrawlerException e) {
                metricsRecorder.recordError(e);
                log.error(NOISY, "Failed to initialize subscriptions", e);
                throw e;
            } catch (Exception e) {
                metricsRecorder.recordError(e);
                log.error(NOISY, "Failed to initialize subscriptions", e);
                throw new SaaSCrawlerException("Failed to initialize subscriptions: " + e.getMessage(), e, true);
            }
        });
    }

    /**
     * Searches for audit logs of a specific content type within a time range.
     * Implements retry with exponential backoff for recoverable errors.
     *
     * @param contentType the type of content to search for
     * @param startTime  the start time of the search range
     * @param endTime    the end time of the search range
     * @param pageUri    the URI for pagination (can be null for first page)
     * @return AuditLogsResponse containing the list of audit log entries and the next page URI
     */
    public AuditLogsResponse searchAuditLogs(final String contentType,
                                             final Instant startTime,
                                             final Instant endTime,
                                             String pageUri) {
        final String url = pageUri != null ? pageUri :
                String.format(GET_AUDIT_LOGS_URL,
                        authConfig.getTenantId(),
                        contentType,
                        startTime.toString(),
                        endTime.toString());

        log.debug("Searching audit logs with URL: {}", url);
        final HttpHeaders headers = new HttpHeaders();

        return metricsRecorder.recordSearchLatency(() -> {
            try {
                return retryHandler.executeWithRetry(
                        () -> {
                            headers.setBearerAuth(authConfig.getAccessToken());
                            metricsRecorder.recordDataApiRequest();

                            ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
                                    url,
                                    HttpMethod.GET,
                                    new HttpEntity<>(headers),
                                    new ParameterizedTypeReference<>() {}
                            );

                            metricsRecorder.recordSearchResponseSize(response);
                            metricsRecorder.recordSearchSuccess();

                            // Extract NextPageUri from response headers
                            List<String> nextPageHeaders = response.getHeaders().get("NextPageUri");
                            String nextPageUri = (nextPageHeaders != null && !nextPageHeaders.isEmpty()) ?
                                    nextPageHeaders.get(0) : null;

                            if (nextPageUri != null) {
                                log.debug("Next page URI found: {}", nextPageUri);
                            }

                            return new AuditLogsResponse(response.getBody(), nextPageUri);
                        },
                        authConfig::renewCredentials,
                        metricsRecorder::recordSearchFailure
                );
            } catch (SaaSCrawlerException e) {
                metricsRecorder.recordError(e);
                log.error(NOISY, "Error while fetching audit logs for content type {} from URL: {}",
                        contentType, url, e);
                throw e;
            } catch (Exception e) {
                metricsRecorder.recordError(e);
                log.error(NOISY, "Error while fetching audit logs for content type {} from URL: {}",
                        contentType, url, e);
                throw new SaaSCrawlerException("Failed to fetch audit logs", e, true);
            }
        });
    }


    /**
     * Retrieves the audit log content from a specific content URI.
     *
     * @param contentUri the URI of the audit log content
     * @return the audit log content as a string
     */
    public String getAuditLog(String contentUri) {
        if (!contentUri.startsWith(MANAGEMENT_API_BASE_URL)) {
            throw new SaaSCrawlerException("ContentUri must be from Office365 Management API: " + contentUri, false);
        }

        log.debug("Getting audit log from content URI: {}", contentUri);
        metricsRecorder.recordLogsRequested();
        final HttpHeaders headers = new HttpHeaders();

        return metricsRecorder.recordGetLatency(() -> {
            try {
                String response = retryHandler.executeWithRetry(() -> {
                    headers.setBearerAuth(authConfig.getAccessToken());
                    metricsRecorder.recordDataApiRequest();
                    ResponseEntity<String> responseEntity = restTemplate.exchange(
                            contentUri,
                            HttpMethod.GET,
                            new HttpEntity<>(headers),
                            String.class
                    );

                    return responseEntity.getBody();
                }, authConfig::renewCredentials, metricsRecorder::recordGetFailure);

                metricsRecorder.recordGetResponseSize(response);
                metricsRecorder.recordGetSuccess();

                return response;
            } catch (SaaSCrawlerException e) {
                metricsRecorder.recordError(e);
                log.error(NOISY, "Error while fetching audit log content from URI: {}", contentUri, e);
                throw e;
            } catch (Exception e) {
                metricsRecorder.recordError(e);
                log.error(NOISY, "Error while fetching audit log content from URI: {}", contentUri, e);
                throw new SaaSCrawlerException("Failed to fetch audit log", e, true);
            }
        });
    }
}
