package org.opensearch.dataprepper.plugins.source.crowdstrike;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import javax.inject.Named;
import java.time.Instant;
import java.util.Iterator;

/**
 * This class represents a CrowdStrike client.
 */
@Named
public class CrowdStrikeClient implements CrawlerClient {

    @Override
    public Iterator<ItemInfo> listItems(Instant lastPollTime) {
        return null;
    }

    @Override
    public void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer, AcknowledgementSet acknowledgementSet) {

    }
}
