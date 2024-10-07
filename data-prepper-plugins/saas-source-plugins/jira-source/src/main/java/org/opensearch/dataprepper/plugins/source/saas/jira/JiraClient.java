package org.opensearch.dataprepper.plugins.source.saas.jira;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasClient;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasSourceConfig;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.PROJECT_KEY;

/**
 * This class represents a Jira client.
 */
@Named
public class JiraClient implements SaasClient {

    private static final Logger log = LoggerFactory.getLogger(JiraClient.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    public static final String PROJECT = "project";

    private final JiraService service;
    private JiraConfiguration configuration;
    private final JiraIterator jiraIterator;
    private long lastPollTime;

    public JiraClient(JiraService service, JiraIterator jiraIterator) {
        this.service = service;
        this.jiraIterator = jiraIterator;
    }



    //@Override
    public String getItem(ItemInfo itemInfo) {
        return service.getIssue(itemInfo.getId(), configuration);
    }


    @Override
    public Iterator<ItemInfo> listItems() {
        jiraIterator.initialize(lastPollTime);
        return jiraIterator;
    }

    @Override
    public void setConfiguration(SaasSourceConfig configuration) {
        this.configuration = JiraConfiguration.of((JiraSourceConfig) configuration);
        this.jiraIterator.setSourceConfig(configuration);
    }

    @Override
    public void setLastPollTime(long lastPollTime) {
        log.info("Setting the lastPollTime: {}", lastPollTime);
        this.lastPollTime = lastPollTime;
    }

    @Override
    public void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer) {
        List<String> itemIds = state.getItemIds();
        Map<String, String> keyAttributes = state.getKeyAttributes();
        String project = keyAttributes.get(PROJECT);
        long eventTime = state.getExportStartTime();
        try {
            //TODO: parallelize this work
            List<Record<Event>> recordsToWrite = new ArrayList<>();
            for(String itemId : itemIds) {
                ItemInfo itemInfo = JiraItemInfo.builder()
                        .withItemId(itemId)
                        .withId(itemId)
                        .withProject(project)
                        .withEventTime(eventTime)
                        .withMetadata(keyAttributes).build();
                String issueJsonString = getItem(itemInfo);
//                    log.info("Entire Json {}", issueJsonString);

                        Map<String, Object> eventData =
                                objectMapper.readValue(issueJsonString, new TypeReference<>() {});
                    Event event = JacksonEvent.builder()
                            .withEventType("Ticket")
                            .withData(eventData)
                            .build();
                    recordsToWrite.add(new Record<>(event));

            }
            buffer.writeAll(recordsToWrite, (int) Duration.ofSeconds(10).toMillis());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
