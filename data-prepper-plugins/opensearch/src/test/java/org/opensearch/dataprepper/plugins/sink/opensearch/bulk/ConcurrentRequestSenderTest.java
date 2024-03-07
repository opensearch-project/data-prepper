package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Random;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ConcurrentRequestSenderTest {
    private static final int CONCURRENT_REQUESTS = 1 + new Random().nextInt(9);

    @Mock
    private AccumulatingBulkRequest request;
    @Mock
    private Consumer<AccumulatingBulkRequest> requestConsumer;
    @Mock
    private CompletionService<Void> completionService;
    @Mock
    private Future<Void> future;

    private ConcurrentRequestSender concurrentRequestSender;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        concurrentRequestSender = new ConcurrentRequestSender(CONCURRENT_REQUESTS, completionService);

        when(completionService.submit(any())).thenReturn(future);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(request, requestConsumer, completionService, future);
    }

    @Test
    void sendRequest_QueueHasSlots_Success() {
        concurrentRequestSender.sendRequest(requestConsumer, request);

        verify(completionService).submit(any());
    }

    @Test
    void sendRequest_NoTakeIfQueueFutureCompleted(){
        final int concurrency = 3;
        concurrentRequestSender = new ConcurrentRequestSender(concurrency, completionService);

        when(future.isCancelled()).thenReturn(false);
        when(future.isDone())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);

        IntStream.range(0, concurrency + 1).forEach(i -> concurrentRequestSender.sendRequest(requestConsumer, request));

        verify(completionService, times(concurrency + 1)).submit(any());
        verify(future, times(concurrency)).isCancelled();
        verify(future, times(concurrency)).isDone();
    }

    @Test
    void sendRequest_NoTake_MultipleFuturesCompleted(){
        final int concurrency = 3;
        concurrentRequestSender = new ConcurrentRequestSender(concurrency, completionService);

        when(future.isCancelled()).thenReturn(false);
        when(future.isDone())
                .thenReturn(false)
                .thenReturn(true)
                .thenReturn(true);

        IntStream.range(0, concurrency + 2).forEach(i -> concurrentRequestSender.sendRequest(requestConsumer, request));

        verify(completionService, times(concurrency + 2)).submit(any());
        verify(future, times(concurrency)).isCancelled();
        verify(future, times(concurrency)).isDone();
    }

    @Test
    void sendRequest_NoTake_FutureCancelled() throws ExecutionException, InterruptedException {
        final int concurrency = 3;
        concurrentRequestSender = new ConcurrentRequestSender(concurrency, completionService);

        when(future.isCancelled())
                .thenReturn(false)
                .thenReturn(true)
                .thenReturn(false);
        when(future.isDone()).thenReturn(false);
        when(future.get()).thenThrow(new InterruptedException());

        IntStream.range(0, concurrency + 1).forEach(i -> concurrentRequestSender.sendRequest(requestConsumer, request));

        verify(completionService, times(concurrency + 1)).submit(any());
        verify(future).get();
        verify(future, times(concurrency)).isCancelled();
        verify(future, times(concurrency - 1)).isDone();
    }

    @Test
    void sendRequest_NoTake_FutureCancelled_NonExceptional() throws ExecutionException, InterruptedException {
        final int concurrency = 3;
        concurrentRequestSender = new ConcurrentRequestSender(concurrency, completionService);

        when(future.isCancelled())
                .thenReturn(false)
                .thenReturn(true)
                .thenReturn(false);
        when(future.isDone()).thenReturn(false);
        when(future.get()).thenReturn(null);

        IntStream.range(0, concurrency + 1).forEach(i -> concurrentRequestSender.sendRequest(requestConsumer, request));

        verify(completionService, times(concurrency + 1)).submit(any());
        verify(future).get();
        verify(future, times(concurrency)).isCancelled();
        verify(future, times(concurrency - 1)).isDone();
    }

    @Test
    void sendRequest_TakesToBlockForFutureCompletion() throws InterruptedException {
        final int concurrency = 3;
        concurrentRequestSender = new ConcurrentRequestSender(concurrency, completionService);

        when(future.isCancelled()).thenReturn(false);
        when(future.isDone())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);
        when(completionService.take()).thenReturn(future);

        IntStream.range(0, concurrency + 1).forEach(i -> concurrentRequestSender.sendRequest(requestConsumer, request));

        verify(completionService, times(concurrency + 1)).submit(any());
        verify(completionService).take();
        verify(future, times(concurrency * 2)).isCancelled();
        verify(future, times(concurrency * 2)).isDone();
    }

    @Test
    void sendRequest_TakesToBlockForFutureCompletion_TransientException() throws InterruptedException {
        final int concurrency = 3;
        concurrentRequestSender = new ConcurrentRequestSender(concurrency, completionService);

        when(future.isCancelled()).thenReturn(false);
        when(future.isDone())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);
        when(completionService.take())
                .thenThrow(new RuntimeException())
                .thenReturn(future);

        IntStream.range(0, concurrency + 1).forEach(i -> concurrentRequestSender.sendRequest(requestConsumer, request));

        verify(completionService, times(concurrency + 1)).submit(any());
        verify(completionService, times(2)).take();
        verify(future, times(concurrency * 3)).isCancelled();
        verify(future, times(concurrency * 3)).isDone();
    }
}
