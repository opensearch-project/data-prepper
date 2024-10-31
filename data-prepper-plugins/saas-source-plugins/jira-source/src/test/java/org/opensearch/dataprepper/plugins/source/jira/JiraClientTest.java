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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
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

    @Test
    void testConstructor() {
        JiraClient jiraClient = new JiraClient(jiraService, jiraIterator, executorServiceProvider, jiraSourceConfig);
        jiraClient.setLastPollTime(Instant.ofEpochSecond(1234L));
        assertNotNull(jiraClient);
    }

    @Test
    void testListItems() {
        JiraClient jiraClient = new JiraClient(jiraService, jiraIterator, executorServiceProvider, jiraSourceConfig);
        assertNotNull(jiraClient.listItems());
    }


    @Test
    void testExecutePartition() throws Exception {
        JiraClient jiraClient = new JiraClient(jiraService, jiraIterator, executorServiceProvider, jiraSourceConfig);
        Map<String, Object> keyAttributes = new HashMap<>();
        keyAttributes.put("project", "test");
        when(saasWorkerProgressState.getKeyAttributes()).thenReturn(keyAttributes);
        List<String> itemIds = new ArrayList<>();
        itemIds.add(null);
        itemIds.add("ID2");
        itemIds.add("ID3");
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

    @Test
    void bufferWriteRuntimeTest() throws Exception {
        JiraClient jiraClient = new JiraClient(jiraService, jiraIterator, executorServiceProvider, jiraSourceConfig);
        Map<String, Object> keyAttributes = new HashMap<>();
        keyAttributes.put("project", "test");
        when(saasWorkerProgressState.getKeyAttributes()).thenReturn(keyAttributes);
        List<String> itemIds = List.of("ID1", "ID2", "ID3", "ID4");
        when(saasWorkerProgressState.getItemIds()).thenReturn(itemIds);
        Instant exportStartTime = Instant.now();
        when(saasWorkerProgressState.getExportStartTime()).thenReturn(Instant.ofEpochSecond(exportStartTime.toEpochMilli()));

        when(jiraService.getIssue(anyString())).thenReturn("{\"id\":\"ID1\",\"key\":\"TEST-1\"}");

        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass((Class) Collection.class);

        doThrow(new RuntimeException()).when(buffer).writeAll(recordsCaptor.capture(), anyInt());
        assertThrows(RuntimeException.class, () -> jiraClient.executePartition(saasWorkerProgressState, buffer, crawlerSourceConfig));
    }
}