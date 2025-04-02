package org.opensearch.dataprepper;

import com.google.protobuf.Any;
import com.google.rpc.RetryInfo;
import com.linecorp.armeria.common.RequestContext;
import io.grpc.Metadata;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CustomAuthenticationExceptionHandlerTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter requestTimeoutsCounter;

    @Mock
    private Counter badRequestsCounter;

    @Mock
    private Counter requestsTooLargeCounter;

    @Mock
    private Counter internalServerErrorCounter;

    @Mock
    private RequestContext requestContext;

    @Mock
    private Metadata metadata;

    private CustomAuthenticationExceptionHandler handler;

    @BeforeEach
    public void setUp() {
        when(pluginMetrics.counter(CustomAuthenticationExceptionHandler.REQUEST_TIMEOUTS)).thenReturn(requestTimeoutsCounter);
        when(pluginMetrics.counter(CustomAuthenticationExceptionHandler.BAD_REQUESTS)).thenReturn(badRequestsCounter);
        when(pluginMetrics.counter(CustomAuthenticationExceptionHandler.REQUESTS_TOO_LARGE)).thenReturn(requestsTooLargeCounter);
        when(pluginMetrics.counter(CustomAuthenticationExceptionHandler.INTERNAL_SERVER_ERROR)).thenReturn(internalServerErrorCounter);

        handler = new CustomAuthenticationExceptionHandler(pluginMetrics, Duration.ofMillis(100), Duration.ofSeconds(2));
    }

    @Test
    public void testBadRequestExceptionHandling() {
        final String message = UUID.randomUUID().toString();
        BadRequestException exception = new BadRequestException(message, new IOException());

        com.google.rpc.Status status = handler.applyStatusProto(requestContext, exception, metadata);

        assertThat(status.getCode(), equalTo(Status.Code.INVALID_ARGUMENT.value()));
        assertThat(status.getMessage(), equalTo(message));
        verify(badRequestsCounter).increment();
    }

    @Test
    public void testTimeoutExceptionHandling() {
        TimeoutException timeout = new TimeoutException();
        BufferWriteException bufferWriteException = new BufferWriteException("timeout", timeout);

        com.google.rpc.Status status = handler.applyStatusProto(requestContext, bufferWriteException, metadata);

        assertThat(status.getCode(), equalTo(Status.Code.RESOURCE_EXHAUSTED.value()));
        verify(requestTimeoutsCounter).increment();
        Optional<Any> retryInfo = status.getDetailsList().stream().filter(d -> d.is(RetryInfo.class)).findFirst();
        assertTrue(retryInfo.isPresent());
    }

    @Test
    public void testSizeOverflowExceptionHandling() {
        SizeOverflowException overflow = new SizeOverflowException("Overflow");
        BufferWriteException bufferWriteException = new BufferWriteException("overflow", overflow);

        com.google.rpc.Status status = handler.applyStatusProto(requestContext, bufferWriteException, metadata);

        assertThat(status.getCode(), equalTo(Status.Code.RESOURCE_EXHAUSTED.value()));
        verify(requestsTooLargeCounter).increment();
    }

    @Test
    public void testCancelledRequestHandling() {
        String message = UUID.randomUUID().toString();
        RequestCancelledException exception = new RequestCancelledException(message);

        com.google.rpc.Status status = handler.applyStatusProto(requestContext, exception, metadata);

        assertThat(status.getCode(), equalTo(Status.Code.CANCELLED.value()));
        assertThat(status.getMessage(), equalTo(message));
        verify(requestTimeoutsCounter).increment();
    }

    @Test
    public void testInternalExceptionHandling() {
        String message = UUID.randomUUID().toString();
        RuntimeException exception = new RuntimeException(message);

        com.google.rpc.Status status = handler.applyStatusProto(requestContext, exception, metadata);

        assertThat(status.getCode(), equalTo(Status.Code.INTERNAL.value()));
        assertThat(status.getMessage(), equalTo(message));
        verify(internalServerErrorCounter).increment();
    }
}
