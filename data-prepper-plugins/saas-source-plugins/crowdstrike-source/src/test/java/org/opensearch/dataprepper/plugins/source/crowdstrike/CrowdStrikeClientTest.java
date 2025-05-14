package org.opensearch.dataprepper.plugins.source.crowdstrike;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.crowdstrike.models.CrowdStrikeIndicatorResult;
import org.opensearch.dataprepper.plugins.source.crowdstrike.models.CrowdStrikeThreatIntelApiResponse;
import org.opensearch.dataprepper.plugins.source.crowdstrike.models.ThreatIndicator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeWorkerProgressState;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CrowdStrikeClientTest {

    @Mock
    private CrowdStrikeService crowdStrikeService;

    @Mock
    private CrowdStrikeSourceConfig configuration;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    @InjectMocks
    private CrowdStrikeClient client;

    @Test
    void testExecutePartition_success() throws Exception {
        CrowdStrikeWorkerProgressState state = new CrowdStrikeWorkerProgressState();
        state.setStartTime(Instant.parse("2024-10-30T00:00:00Z"));
        state.setEndTime(Instant.parse("2024-10-30T01:00:00Z"));

        ThreatIndicator indicator = new ThreatIndicator();
        indicator.setType("ipv4");
        indicator.setIndicator("1.2.3.4");
        indicator.setPublishedDate(Instant.parse("2024-10-30T00:00:00Z"));
        indicator.setId("indicator-1");

        CrowdStrikeIndicatorResult result = new CrowdStrikeIndicatorResult();
        result.setResults(List.of(indicator));

        CrowdStrikeThreatIntelApiResponse response = mock(CrowdStrikeThreatIntelApiResponse.class);
        when(response.getBody()).thenReturn(result);
        when(crowdStrikeService.getThreatIndicators(any(), any(), any()))
                .thenReturn(response);
        when(configuration.isAcknowledgments()).thenReturn(true);

        client.executePartition(state, buffer, acknowledgementSet);

        verify(buffer, times(1)).writeAll(anyList(), anyInt());
        verify(acknowledgementSet, times(1)).complete();
    }

    @Test
    void testExecutePartition_writeFails() throws Exception {
        CrowdStrikeWorkerProgressState state = new CrowdStrikeWorkerProgressState();
        state.setStartTime(Instant.now());
        state.setEndTime(Instant.now());

        CrowdStrikeIndicatorResult result = new CrowdStrikeIndicatorResult();
        result.setResults(List.of(new ThreatIndicator()));

        CrowdStrikeThreatIntelApiResponse response = mock(CrowdStrikeThreatIntelApiResponse.class);
        when(response.getBody()).thenReturn(result);
        when(crowdStrikeService.getThreatIndicators(any(), any(), any()))
                .thenReturn(response);
        doThrow(RuntimeException.class).when(buffer).writeAll(anyList(), anyInt());

        assertThrows(RuntimeException.class, () ->
                client.executePartition(state, buffer, acknowledgementSet));
        verify(acknowledgementSet, never()).complete();
    }

    @Test
    void testExecutePartition_withPagination() throws Exception {
        CrowdStrikeWorkerProgressState state = new CrowdStrikeWorkerProgressState();
        state.setStartTime(Instant.now());
        state.setEndTime(Instant.now());

        CrowdStrikeIndicatorResult firstBatch = new CrowdStrikeIndicatorResult();
        firstBatch.setResults(List.of(new ThreatIndicator()));

        CrowdStrikeIndicatorResult secondBatch = new CrowdStrikeIndicatorResult();
        secondBatch.setResults(List.of(new ThreatIndicator()));

        CrowdStrikeThreatIntelApiResponse response1 = mock(CrowdStrikeThreatIntelApiResponse.class);
        when(response1.getBody()).thenReturn(firstBatch);
        when(response1.getFirstHeaderValue("Next-Page")).thenReturn(Optional.of("nextLink"));

        CrowdStrikeThreatIntelApiResponse response2 = mock(CrowdStrikeThreatIntelApiResponse.class);
        when(response2.getBody()).thenReturn(secondBatch);
        when(response2.getFirstHeaderValue("Next-Page")).thenReturn(Optional.empty());

        when(crowdStrikeService.getThreatIndicators(any(), any(), eq(Optional.empty())))
                .thenReturn(response1);
        when(crowdStrikeService.getThreatIndicators(any(), any(), eq(Optional.of("nextLink"))))
                .thenReturn(response2);

        client.executePartition(state, buffer, acknowledgementSet);

        verify(buffer, times(2)).writeAll(anyList(), anyInt());
    }
}
