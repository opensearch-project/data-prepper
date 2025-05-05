package org.opensearch.dataprepper.plugins.source.crowdstrike;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.crowdstrike.models.CrowdStrikeIndicatorResult;
import org.opensearch.dataprepper.plugins.source.crowdstrike.models.CrowdStrikeThreatIntelApiResponse;
import org.opensearch.dataprepper.plugins.source.crowdstrike.models.ThreatIndicator;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.inject.Named;
import java.time.Instant;
import java.util.Iterator;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.NEXT_PAGE;

/**
 * This class represents a CrowdStrike client.
 */
@Named
public class CrowdStrikeClient implements CrawlerClient<CrowdStrikeWorkerProgressState> {
    CrowdStrikeService crowdStrikeService;
    private static final Logger log = LoggerFactory.getLogger(CrowdStrikeClient.class);
    private final CrowdStrikeSourceConfig configuration;
    private final int bufferWriteTimeoutInSeconds = 10;


    public CrowdStrikeClient(CrowdStrikeService crowdStrikeService,
                             CrowdStrikeSourceConfig sourceConfig) {
        log.info("Creating CrowdStrike Crawler");
        this.crowdStrikeService = crowdStrikeService;
        this.configuration = sourceConfig;
        log.info("Created CrowdStrike Crawler");
    }
    @Override
    public Iterator<ItemInfo> listItems(Instant lastPollTime) {
        return null;
    }

    /**
     * Executes the data ingestion process for a given time-based partition of CrowdStrike threat intelligence data.
     * This method fetches threat indicators from the CrowdStrike API for the specified time range defined in
     * CrowdStrikeWorkerProgressState. It handles pagination using the `next-page` link header and continues
     * fetching data until all pages are exhausted. Each batch of threat indicators is converted into
     * JacksonEvent records and written to the provided Buffer
     */
    @Override
    public void executePartition(CrowdStrikeWorkerProgressState state, Buffer<Record<Event>> buffer, AcknowledgementSet acknowledgementSet) {
        final Instant startTime = state.getStartTime();
        final Instant endTime = state.getEndTime();
        Optional<String> paginationLink = Optional.empty();

        do {
            CrowdStrikeThreatIntelApiResponse response = crowdStrikeService.getThreatIndicators(startTime, endTime, paginationLink);
            CrowdStrikeIndicatorResult result = response.getBody();
            List<ThreatIndicator> indicators = result.getResults();
            if (indicators == null || indicators.isEmpty()) {
                log.info("No threat indicators found for the time window {} to {}", startTime, endTime);
            } else {
                writeIndicatorsToBuffer(indicators, buffer);
            }
            paginationLink = response.getFirstHeaderValue(NEXT_PAGE);
        } while (paginationLink.isPresent());

        if (configuration.isAcknowledgments()) {
            acknowledgementSet.complete();
        }
    }

    private void writeIndicatorsToBuffer(List<ThreatIndicator> indicators, Buffer<Record<Event>> buffer) {
        List<Record<Event>> records = indicators.parallelStream()
                .map(indicator -> (Event) JacksonEvent.builder()
                        .withEventType(EventType.DOCUMENT.toString())
                        .withData(indicator)
                        .build())
                .map(Record::new)
                .collect(Collectors.toList());

        try {
            buffer.writeAll(records, (int) Duration.ofSeconds(bufferWriteTimeoutInSeconds).toMillis());
        } catch (Exception e) {
            log.error("Failed to write {} indicators to buffer", records.size(), e);
            throw new RuntimeException("Buffer write failed for CrowdStrike indicators", e);
        }
    }
}
