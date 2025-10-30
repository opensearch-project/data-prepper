package org.opensearch.dataprepper.plugins.source.oteltrace.http;


import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.RequestTimeoutException;
import com.linecorp.armeria.server.ServiceRequestContext;

import io.micrometer.core.instrument.Counter;

@ExtendWith(MockitoExtension.class)
class HttpExceptionHandlerTest {
    HttpExceptionHandler httpExceptionHandler;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ServiceRequestContext requestContext;

    @Mock
    private HttpRequest httpRequest;

    @Mock
    private Counter requestTimeoutsCounter;

    @Mock
    private Counter badRequestsCounter;

    @Mock
    private Counter requestsTooLargeCounter;

    @Mock
    private Counter internalServerErrorCounter;
    @BeforeEach
    public void setUp() {
        when(pluginMetrics.counter(HttpRequestExceptionHandler.REQUEST_TIMEOUTS)).thenReturn(requestTimeoutsCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.BAD_REQUESTS)).thenReturn(badRequestsCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.REQUESTS_TOO_LARGE)).thenReturn(requestsTooLargeCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.INTERNAL_SERVER_ERROR)).thenReturn(internalServerErrorCounter);
        httpExceptionHandler = new HttpExceptionHandler(pluginMetrics, Duration.ofMillis(100), Duration.ofSeconds(2));
    }

    @Test
    public void testHandleBadRequestException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, new BadRequestException("msg", null));
        verify(badRequestsCounter).increment();
    }

    @Test
    public void testHandleTimeoutException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, new BufferWriteException(null, new TimeoutException()));
        verify(requestTimeoutsCounter, times(1)).increment();
    }

    @Test
    public void testHandleArmeriaTimeoutException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, RequestTimeoutException.get());
        verify(requestTimeoutsCounter, times(1)).increment();
    }

    @Test
    public void testHandleSizeOverflowException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, new SizeOverflowException("msg"));
        verify(requestsTooLargeCounter).increment();
    }

    @Test
    public void testHandleRequestCancelledException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, new RequestCancelledException("msg"));
        verify(requestTimeoutsCounter, times(1)).increment();
    }

    @Test
    public void testHandleInternalServerException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, new RuntimeException("msg"));
        verify(internalServerErrorCounter, times(1)).increment();
    }
}
