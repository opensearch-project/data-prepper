package org.opensearch.dataprepper.plugins.source.saas.jira;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasClient;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasPluginExecutorServiceProvider;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * This class represents a Jira client.
 */
@Named
public class JiraClient implements SaasClient {

    private static final Logger log = LoggerFactory.getLogger(JiraClient.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    public static final String PROJECT = "project";

    private final JiraService service;
    private SaasSourceConfig configuration;
    private final JiraIterator jiraIterator;
    private long lastPollTime;
    private final ExecutorService executorService;

    public JiraClient(JiraService service,
                      JiraIterator jiraIterator,
                      SaasPluginExecutorServiceProvider executorServiceProvider,
                      JiraSourceConfig sourceConfig) {
        this.service = service;
        this.jiraIterator = jiraIterator;
        this.executorService = executorServiceProvider.get();
        this.configuration = JiraConfiguration.of(sourceConfig);
    }


    //@Override
    public String getItem(ItemInfo itemInfo, SaasSourceConfig configuration) {
        return service.getIssue(itemInfo.getId(), (JiraSourceConfig) configuration);
    }


    @Override
    public Iterator<ItemInfo> listItems() {
        jiraIterator.initialize(lastPollTime);
        return jiraIterator;
    }

    @Override
    public void setConfiguration(SaasSourceConfig configuration) {
        this.configuration = configuration;
        this.jiraIterator.setSourceConfig(configuration);
    }

    @Override
    public void setLastPollTime(long lastPollTime) {
        log.info("Setting the lastPollTime: {}", lastPollTime);
        this.lastPollTime = lastPollTime;
    }

    @Override
    public void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer, SaasSourceConfig configuration) {
        List<String> itemIds = state.getItemIds();
        Map<String, String> keyAttributes = state.getKeyAttributes();
        String project = keyAttributes.get(PROJECT);
        long eventTime = state.getExportStartTime();
        List<ItemInfo> itemInfos = new ArrayList<>();
        for(String itemId : itemIds) {
            if (itemId == null) {
                continue;
            }
            ItemInfo itemInfo = JiraItemInfo.builder()
                    .withItemId(itemId)
                    .withId(itemId)
                    .withProject(project)
                    .withEventTime(eventTime)
                    .withMetadata(keyAttributes).build();
            itemInfos.add(itemInfo);
        }

        List<Record<Event>> recordsToWrite = itemInfos
                .parallelStream()
                .map(t -> (Supplier<String>) (() -> getItem(t, configuration)))
                .map(supplier -> supplyAsync(supplier, this.executorService))
                .map(CompletableFuture::join)
                .map(ticketJson -> {
                    try {
                        return objectMapper.readValue(ticketJson, new TypeReference<>() {
                        });
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(t -> (Event)JacksonEvent.builder()
                        .withEventType("Ticket")
                        .withData(t)
                        .build())
                .map(event -> new Record<>(event))
                .collect(Collectors.toList());

        try {
            buffer.writeAll(recordsToWrite, (int) Duration.ofSeconds(10).toMillis());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
