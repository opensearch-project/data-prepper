package org.opensearch.dataprepper.plugins.source.crowdstrike.rest;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.UnauthorizedException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import java.net.URI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CrowdStrikeRestClientTest {
    private CrowdStrikeAuthClient authClient;
    private PluginMetrics pluginMetrics;
    private Counter counter;
    private RestTemplate restTemplate;
    private CrowdStrikeRestClient restClient;
    private URI uri;
    private static final String BEARER_TOKEN = "mock-token";

    @BeforeEach
    void setup() throws Exception {
        authClient = mock(CrowdStrikeAuthClient.class);
        pluginMetrics = mock(PluginMetrics.class);
        counter = mock(Counter.class);
        when(pluginMetrics.counter(anyString())).thenReturn(counter);
        when(authClient.getBearerToken()).thenReturn(BEARER_TOKEN);

        restTemplate = mock(RestTemplate.class);
        uri = new URI("https://api.crowdstrike.com/intel/combined/indicators/v1");

        restClient = new CrowdStrikeRestClient(pluginMetrics, authClient);
        var restTemplateField = CrowdStrikeRestClient.class.getDeclaredField("restTemplate");
        restTemplateField.setAccessible(true);
        restTemplateField.set(restClient, restTemplate);
    }

    @Test
    void testSuccessfulApiCall() {
        ResponseEntity<String> mockResponse = new ResponseEntity<>("OK", HttpStatus.OK);
        when(restTemplate.exchange(any(), eq(HttpMethod.GET), any(), eq(String.class))).thenReturn(mockResponse);

        ResponseEntity<String> response = restClient.invokeGetApi(uri, String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("OK", response.getBody());
    }

    @Test
    void testUnauthorizedThenSuccessAfterRefresh() {
        ResponseEntity<String> mockResponse = new ResponseEntity<>("OK", HttpStatus.OK);

        when(restTemplate.exchange(any(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.UNAUTHORIZED))
                .thenReturn(mockResponse);

        ResponseEntity<String> response = restClient.invokeGetApi(uri, String.class);

        assertEquals("OK", response.getBody());
        verify(authClient, times(1)).refreshToken();
        verify(counter, times(1)).increment();
    }

    @Test
    void testTooManyRequestsBacksOffAndRetries() {
        ResponseEntity<String> mockResponse = new ResponseEntity<>("OK", HttpStatus.OK);

        when(restTemplate.exchange(any(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS))
                .thenReturn(mockResponse);

        ResponseEntity<String> response = restClient.invokeGetApi(uri, String.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    void testForbiddenFailsImmediately() {
        when(restTemplate.exchange(any(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.FORBIDDEN, "Access Denied"));

        assertThrows(UnauthorizedException.class, () -> restClient.invokeGetApi(uri, String.class));
    }

    @Test
    void testMaxRetriesExceededThrows() {
        when(restTemplate.exchange(any(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> restClient.invokeGetApi(uri, String.class));

        assertTrue(ex.getMessage().contains("Exceeded max retry attempts"));
    }

    @Test
    void testInterruptedSleepThrows() {
        Thread.currentThread().interrupt(); // simulate interrupt

        when(restTemplate.exchange(any(), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS));

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> restClient.invokeGetApi(uri, String.class));

        assertTrue(ex.getMessage().contains("Sleep in the retry attempt got interrupted"));
    }
}
