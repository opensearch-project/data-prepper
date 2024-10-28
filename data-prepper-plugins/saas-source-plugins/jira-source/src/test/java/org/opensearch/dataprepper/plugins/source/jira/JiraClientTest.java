package org.opensearch.dataprepper.plugins.source.jira;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.SaasWorkerProgressState;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JiraClientTest {

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private SaasWorkerProgressState saasWorkerProgressState;

    @Mock
    private CrawlerSourceConfig crawlerSourceConfig;

    @Mock
    private JiraSourceConfig jiraSourceConfig;

    @Mock
    private JiraService jiraService;

    @Mock
    private JiraIterator jiraIterator;

    private PluginExecutorServiceProvider executorServiceProvider = new PluginExecutorServiceProvider();
    private JiraClient jiraClient;

    @Test
    void testConstructor() {
        jiraClient = new JiraClient(jiraService, jiraIterator, executorServiceProvider, jiraSourceConfig);
        assertNotNull(jiraClient);
    }

    @Test
    void testListItems() {
        jiraClient = new JiraClient(jiraService, jiraIterator, executorServiceProvider, jiraSourceConfig);
        assertNotNull(jiraClient.listItems());
    }

    @Test
    void testSetLastPollTime() throws NoSuchFieldException, IllegalAccessException {
        jiraClient = new JiraClient(jiraService, jiraIterator, executorServiceProvider, jiraSourceConfig);
        jiraClient.setLastPollTime(Instant.ofEpochSecond(1234L));
        Field pollTime = jiraClient.getClass().getDeclaredField("lastPollTime");
        pollTime.setAccessible(true);
        Object oldPollTime = pollTime.get(jiraClient);
        jiraClient.setLastPollTime(Instant.ofEpochSecond(5678L));
        Object newPollTime = pollTime.get(jiraClient);

        assertNotEquals(oldPollTime, newPollTime);
        assertEquals(Instant.ofEpochSecond(5678L), newPollTime);
    }

    @Test
    void testExecutePartition() throws Exception {
        jiraClient = new JiraClient(jiraService, jiraIterator, executorServiceProvider, jiraSourceConfig);
        Map<String, Object> keyAttributes = new HashMap<>();
        keyAttributes.put("project", "test");
        when(saasWorkerProgressState.getKeyAttributes()).thenReturn(keyAttributes);
        List<String> itemIds = List.of("ID1", "ID2", "ID3", "ID4");
        when(saasWorkerProgressState.getItemIds()).thenReturn(itemIds);
        Instant exportStartTime = Instant.now();
        when(saasWorkerProgressState.getExportStartTime()).thenReturn(Instant.ofEpochSecond(exportStartTime.toEpochMilli()));

        when(jiraService.getIssue(anyString())).thenReturn("{\"id\":\"ID1\",\"key\":\"TEST-1\"}");

        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass((Class) Collection.class);

        jiraClient.executePartition(saasWorkerProgressState, buffer, crawlerSourceConfig);

        verify(buffer).writeAll(recordsCaptor.capture(), anyInt());
        Collection<Record<Event>> capturedRecords = recordsCaptor.getValue();
        assertFalse(capturedRecords.isEmpty());
        for (Record<Event> record : capturedRecords) {
            assertNotNull(record.getData());
        }
    }
}