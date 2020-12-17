package com.amazon.dataprepper.server;

import com.amazon.dataprepper.DataPrepper;
import com.amazon.dataprepper.pipeline.Pipeline;
import com.amazon.dataprepper.pipeline.server.DataPrepperServer;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DataPrepperServerTest {

    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private DataPrepperServer dataPrepperServer;

    private void setRegistry(PrometheusMeterRegistry prometheusMeterRegistry) {
        Metrics.globalRegistry.getRegistries().iterator().forEachRemaining(meterRegistry -> Metrics.globalRegistry.remove(meterRegistry));
        Metrics.addRegistry(prometheusMeterRegistry);
    }

    @Before
    public void initMetrics() {
        setRegistry(new PrometheusRegistryMockScrape(PrometheusConfig.DEFAULT, ""));
    }

    @After
    public void stopServer() {
        dataPrepperServer.stop(0);
    }

    @Test
    public void testGetMetrics() throws IOException, InterruptedException, URISyntaxException {
        final String scrape = UUID.randomUUID().toString();
        final PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusRegistryMockScrape(PrometheusConfig.DEFAULT, scrape);
        setRegistry(prometheusMeterRegistry);
        dataPrepperServer = new DataPrepperServer(8080, null);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:8080/metrics/prometheus"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        Assert.assertEquals(scrape, response.body());
        dataPrepperServer.stop(0);
    }

    @Test
    public void testScrapeFailure() throws IOException, InterruptedException, URISyntaxException {
        setRegistry(new PrometheusRegistryThrowingScrape(PrometheusConfig.DEFAULT));
        dataPrepperServer = new DataPrepperServer(8080, null);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:8080/metrics/prometheus"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.statusCode());
        dataPrepperServer.stop(0);
    }

    @Test
    public void testListPipelines() throws URISyntaxException, IOException, InterruptedException {
        final DataPrepper mockDataPrepper = Mockito.mock(DataPrepper.class);
        final String pipelineName = "testPipeline";
        Mockito.when(mockDataPrepper.getTransformationPipelines()).thenReturn(
                Collections.singletonMap(pipelineName, Mockito.mock(Pipeline.class))
        );
        dataPrepperServer = new DataPrepperServer(8080, mockDataPrepper);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:8080/list"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        final String expectedResponse = OBJECT_MAPPER.writeValueAsString(
                Collections.singletonMap("pipelines", Arrays.asList(
                        Collections.singletonMap("name", pipelineName)
                ))
        );
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        Assert.assertEquals(expectedResponse, response.body());
        dataPrepperServer.stop(0);
    }

    @Test
    public void testListPipelinesFailure() throws URISyntaxException, IOException, InterruptedException {
        final DataPrepper mockDataPrepper = Mockito.mock(DataPrepper.class);
        Mockito.when(mockDataPrepper.getTransformationPipelines()).thenThrow(new RuntimeException(""));
        dataPrepperServer = new DataPrepperServer(8080, mockDataPrepper);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:8080/list"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.statusCode());
        dataPrepperServer.stop(0);
    }

    @Test
    public void testShutdown() throws URISyntaxException, IOException, InterruptedException {
        final DataPrepper mockDataPrepper = Mockito.mock(DataPrepper.class);
        dataPrepperServer = new DataPrepperServer(8080, mockDataPrepper);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:8080/shutdown"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());
        Mockito.verify(mockDataPrepper).shutdown();
        dataPrepperServer.stop(0);
    }

    @Test
    public void testShutdownFailure() throws URISyntaxException, IOException, InterruptedException {
        final DataPrepper mockDataPrepper = Mockito.mock(DataPrepper.class);
        Mockito.doThrow(new RuntimeException("")).when(mockDataPrepper).shutdown();
        dataPrepperServer = new DataPrepperServer(8080, mockDataPrepper);
        dataPrepperServer.start();

        HttpRequest request = HttpRequest.newBuilder(new URI("http://127.0.0.1:8080/shutdown"))
                .GET().build();
        HttpResponse response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        Assert.assertEquals(HttpURLConnection.HTTP_INTERNAL_ERROR, response.statusCode());
        Mockito.verify(mockDataPrepper).shutdown();
        dataPrepperServer.stop(0);
    }

    private static class PrometheusRegistryMockScrape extends PrometheusMeterRegistry {
        final String scrape;
        public PrometheusRegistryMockScrape(PrometheusConfig config, final String scrape) {
            super(config);
            this.scrape = scrape;
        }

        @Override
        public String scrape() {
            return scrape;
        }
    }

    private static class PrometheusRegistryThrowingScrape extends PrometheusMeterRegistry {

        public PrometheusRegistryThrowingScrape(PrometheusConfig config) {
            super(config);
        }

        @Override
        public String scrape() {
            throw new RuntimeException("");
        }
    }

}
