/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.server;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.SinkModel;
import org.opensearch.dataprepper.pipeline.PipelinesProvider;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GetTransformedPipelinesBodyHandlerTest {
    @Mock
    private PipelinesProvider pipelinesProvider;
    @Mock
    private HttpExchange httpExchange;

    @Mock
    private OutputStream outputStream;

    @BeforeEach
    public void beforeEach() {
        when(httpExchange.getResponseBody())
                .thenReturn(outputStream);
    }

    private GetTransformedPipelinesBodyHandler createObjectUnderTest() {
        return new GetTransformedPipelinesBodyHandler(pipelinesProvider);
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.GET, HttpMethod.POST })
    public void testGivenNoPipelinesThenResponseWritten(String httpMethod) throws IOException {
        final String pipelineName = "test-pipeline";
        final Headers headers = mock(Headers.class);
        final DataPrepperVersion version = DataPrepperVersion.parse("2.0");
        final PluginModel source = new PluginModel("testSource", (Map<String, Object>) null);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", (Map<String, Object>) null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(version, Collections.singletonMap(pipelineName, pipelineModel));

        when(pipelinesProvider.getPipelinesDataFlowModel())
                .thenReturn(pipelinesDataFlowModel);
        when(httpExchange.getResponseHeaders())
                .thenReturn(headers);
        when(httpExchange.getRequestMethod())
                .thenReturn(httpMethod);

        final GetTransformedPipelinesBodyHandler handler = createObjectUnderTest();

        handler.handle(httpExchange);

        verify(headers)
                .add(eq("Content-Type"), eq("text/plain; charset=UTF-8"));
        verify(httpExchange)
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        verify(outputStream)
                .write(any(byte[].class));
        verify(outputStream)
                .close();
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.GET, HttpMethod.POST })
    public void testGivenPipelinesThenResponseWritten(String httpMethod) throws IOException {
        final String pipelineName = "test-pipeline";
        final Headers headers = mock(Headers.class);
        doNothing().when(headers).add(anyString(), anyString());
        final DataPrepperVersion version = DataPrepperVersion.parse("2.0");
        final PluginModel source = new PluginModel("testSource", (Map<String, Object>) null);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", (Map<String, Object>) null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(version, Collections.singletonMap(pipelineName, pipelineModel));

        when(pipelinesProvider.getPipelinesDataFlowModel())
                .thenReturn(pipelinesDataFlowModel);
        when(httpExchange.getResponseHeaders())
                .thenReturn(headers);
        when(httpExchange.getRequestMethod())
                .thenReturn(httpMethod);

        final GetTransformedPipelinesBodyHandler handler = createObjectUnderTest();

        handler.handle(httpExchange);

        verify(headers)
                .add(eq("Content-Type"), eq("text/plain; charset=UTF-8"));
        verify(httpExchange)
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_OK), anyLong());
        verify(outputStream)
                .write(any(byte[].class));
        verify(outputStream)
                .close();
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.DELETE, HttpMethod.PATCH, HttpMethod.PUT })
    public void testGivenProhibitedHttpMethodThenErrorResponseWritten(String httpMethod) throws IOException {
        final GetTransformedPipelinesBodyHandler handler = createObjectUnderTest();

        when(httpExchange.getRequestMethod())
                .thenReturn(httpMethod);

        handler.handle(httpExchange);

        verify(httpExchange)
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_METHOD), eq(0L));
        verify(outputStream)
                .close();
    }

    @ParameterizedTest
    @ValueSource(strings = { HttpMethod.GET, HttpMethod.POST })
    public void testGivenExceptionThrownThenErrorResponseWritten(String httpMethod) throws IOException {
        when(httpExchange.getRequestMethod())
                .thenReturn(httpMethod);

        pipelinesProvider = null;
        final GetTransformedPipelinesBodyHandler handler = createObjectUnderTest();
        handler.handle(httpExchange);

        verify(httpExchange)
                .sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), eq(0L));
        verify(outputStream)
                .close();
    }
}
