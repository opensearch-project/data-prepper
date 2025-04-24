package org.opensearch.dataprepper.plugins.source.crowdstrike;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.crowdstrike.models.CrowdStrikeApiResponse;
import org.opensearch.dataprepper.plugins.source.crowdstrike.models.CrowdStrikeIndicatorResult;
import org.opensearch.dataprepper.plugins.source.crowdstrike.rest.CrowdStrikeRestClient;
import io.micrometer.core.instrument.Timer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import java.net.URI;
import java.util.function.Supplier;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * CrowdStrike service Test
 */
class CrowdStrikeServiceTest {

    private CrowdStrikeRestClient restClient;
    private PluginMetrics pluginMetrics;
    private CrowdStrikeService service;

    @BeforeEach
    void setup() {
        restClient = mock(CrowdStrikeRestClient.class);
        pluginMetrics = mock(PluginMetrics.class);
        Timer mockTimer = mock(Timer.class);
        when(pluginMetrics.timer(any())).thenReturn(mockTimer);
        // Timer records calls synchronously for test
        lenient().when(mockTimer.record(Mockito.<Supplier<Object>>any()))
                .thenAnswer(invocation -> {
                    Supplier<Object> supplier = invocation.getArgument(0);
                    return supplier.get();
                });
        service = new CrowdStrikeService(restClient, pluginMetrics);
    }

    @Test
    void testGetAllContentWithValidTimeRange() {
        CrowdStrikeIndicatorResult result = new CrowdStrikeIndicatorResult();
        ResponseEntity<CrowdStrikeIndicatorResult> responseEntity = ResponseEntity.ok()
                .headers(new HttpHeaders())
                .body(result);

        when(restClient.invokeGetApi(any(), eq(CrowdStrikeIndicatorResult.class)))
                .thenReturn(responseEntity);

        CrowdStrikeApiResponse response = service.getAllContent(1745000000L, 1745003600L, null);

        assertNotNull(response.getBody());
        assertNotNull(response.getHeaders());
        verify(restClient, times(1)).invokeGetApi(any(), eq(CrowdStrikeIndicatorResult.class));
    }

    @Test
    void testGetAllContentWithPaginationLink() throws Exception {
        String paginationLink = "intel/combined/indicators/v1?filter=last_updated%3A%3E%3D1745%2B_marker%3A%3C%27123%27";
        URI sanitizedUri = new URI("https://api.crowdstrike.com/" + paginationLink);
        CrowdStrikeIndicatorResult result = new CrowdStrikeIndicatorResult();

        ResponseEntity<CrowdStrikeIndicatorResult> responseEntity = ResponseEntity.ok()
                .headers(new HttpHeaders())
                .body(result);

        when(restClient.invokeGetApi(eq(sanitizedUri), eq(CrowdStrikeIndicatorResult.class)))
                .thenReturn(responseEntity);

        CrowdStrikeApiResponse response = service.getAllContent(null, null, paginationLink);
        assertNotNull(response.getBody());
        verify(restClient).invokeGetApi(eq(sanitizedUri), eq(CrowdStrikeIndicatorResult.class));
    }

    @Test
    void testRestClientThrowsException() {
        when(restClient.invokeGetApi(any(), eq(CrowdStrikeIndicatorResult.class)))
                .thenThrow(new RuntimeException("API failure"));

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                service.getAllContent(1745000000L, 1745003600L, null));

        assertTrue(ex.getMessage().contains("CrowdStrike API call failed"));
    }

    @Test
    void testBuildUriFailsGracefully() {
        CrowdStrikeService faultyService = new CrowdStrikeService(restClient, pluginMetrics) {
            @Override
            protected URI buildCrowdStrikeUri(Long startTime, Long endTime, String paginationLink) {
                throw new RuntimeException("URI construction failed");
            }
        };

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                faultyService.getAllContent(1745000000L, 1745003600L, null));

        assertEquals("URI construction failed", ex.getMessage());
    }

}
