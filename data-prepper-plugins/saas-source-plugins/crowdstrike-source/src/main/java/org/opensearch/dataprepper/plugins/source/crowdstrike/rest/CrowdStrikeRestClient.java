package org.opensearch.dataprepper.plugins.source.crowdstrike.rest;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import javax.inject.Named;
import java.net.URI;
import java.util.Collections;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.MAX_RETRIES;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.RETRY_ATTEMPT_SLEEP_TIME;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.SLEEP_TIME_MULTIPLIER;

/**
 * Client for interacting with the CrowdStrike Threat Intel REST API.
 */
@Named
public class CrowdStrikeRestClient {

    static final String AUTH_FAILURES_COUNTER = "authFailures";
    private static final Logger log = LoggerFactory.getLogger(CrowdStrikeRestClient.class);
    private final Counter authFailures;
    private final RestTemplate restTemplate;
    private final CrowdStrikeAuthClient authClient;

    public CrowdStrikeRestClient(PluginMetrics pluginMetrics, CrowdStrikeAuthClient authClient) {
        this.authFailures = pluginMetrics.counter(AUTH_FAILURES_COUNTER);
        this.restTemplate = new RestTemplate();
        this.authClient = authClient;
    }

    /**
     * Executes a GET request to the specified CrowdStrike API URI with bearer token authentication
     * and retries in case of transient failures (e.g., 401 Unauthorized, 429 Too Many Requests).
     *
     * <p>Retry strategy:
     * <ul>
     *     <li>Retries up to {@code maxRetries} times</li>
     *     <li>Uses exponential backoff only for 429 (Too Many Requests)</li>
     *     <li>Refreshes token on 401 (Unauthorized)</li>
     *     <li>Fails fast on 403 (Forbidden)</li>
     * </ul>
     *
     * @param uri          The target URI of the CrowdStrike API endpoint
     * @param responseType The expected response body type to map the result into
     * @param <T>          The type of the response body
     * @return ResponseEntity containing the API response body and headers
     * @throws UnauthorizedException if the API returns 403 (Forbidden)
     * @throws RuntimeException if all retries are exhausted or unexpected errors occur
     */
    public <T> ResponseEntity<T> invokeGetApi(URI uri, Class<T> responseType) {
        int retryCount = 0;
        // Create headers
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(authClient.getBearerToken());
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        HttpEntity<?> requestEntity = new HttpEntity<>(headers);
        while (retryCount < MAX_RETRIES) {
            try {
                return restTemplate.exchange(uri, HttpMethod.GET, requestEntity, responseType);
            } catch (HttpClientErrorException ex) {
                HttpStatus statusCode = ex.getStatusCode();
                String statusMessage = ex.getMessage();
                log.error("An exception has occurred while getting response from search API  {}", ex.getMessage());
                if (statusCode == HttpStatus.FORBIDDEN) {
                    throw new UnauthorizedException(statusMessage);
                } else if (statusCode == HttpStatus.UNAUTHORIZED) {
                    authFailures.increment();
                    log.warn(NOISY, "Token expired. We will try to renew the tokens now", ex);
                    authClient.refreshToken();
                } else if (statusCode == HttpStatus.TOO_MANY_REQUESTS) {
                    log.error(NOISY, "Hitting API rate limit. Backing off with sleep timer.", ex);
                }
                try {
                    Thread.sleep((long) RETRY_ATTEMPT_SLEEP_TIME.get(retryCount) * SLEEP_TIME_MULTIPLIER);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Sleep in the retry attempt got interrupted", e);
                }
            }
            retryCount++;
        }
        String errorMessage = String.format("Exceeded max retry attempts. Failed to execute the Rest API call %s", uri);
        log.error(errorMessage);
        throw new RuntimeException(errorMessage);
    }
}
