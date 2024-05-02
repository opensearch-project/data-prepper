package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.linecorp.armeria.client.retry.Backoff;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.ShardStatistics;
import org.opensearch.client.opensearch.core.search.HitsMetadata;
import org.opensearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class SearchRetryStrategy {

    private final RestHighLevelClient restHighLevelClient;
    private final OpenSearchClient openSearchClient;
    static final long INITIAL_DELAY_MS = 50;
    static final long MAXIMUM_DELAY_MS = Duration.ofMinutes(10).toMillis();
    private final int maxRetries;
    private static final Logger LOG = LoggerFactory.getLogger(SearchRetryStrategy.class);

    private static final Set<Integer> NON_RETRY_STATUS = new HashSet<>(
            Arrays.asList(
                    RestStatus.BAD_REQUEST.getStatus(),
                    RestStatus.NOT_FOUND.getStatus(),
                    RestStatus.CONFLICT.getStatus()
            ));


    public SearchRetryStrategy(RestHighLevelClient restHighLevelClient, OpenSearchClient openSearchClient, final int maxRetries) {
        this.restHighLevelClient = restHighLevelClient;
        this.openSearchClient = openSearchClient;
        this.maxRetries = maxRetries;
    }

    public SearchResponse executeSearchRequest(SearchRequest request) throws InterruptedException {
        final Backoff backoff = Backoff.exponential(INITIAL_DELAY_MS, MAXIMUM_DELAY_MS).withMaxAttempts(maxRetries);
        SearchResponse response = null;
        List<SearchResponse> responses = new ArrayList<>();
        int attempt = 1;
        do {
            response = handleRetry(request, attempt, responses);
            if (response != null) {
                final long delayMillis = backoff.nextDelayMillis(attempt++);
                if (delayMillis < 0) {
                    RuntimeException e = new RuntimeException(String.format("Number of retries reached the limit of max retries (configured value %d)", maxRetries));
                    response = handleFailures(request, e);
                    break;
                }
                // Wait for backOff duration
                try {
                    Thread.sleep(delayMillis);
                } catch (final InterruptedException e){
                    LOG.error("Thread is interrupted while attempting to bulk write to OpenSearch with retry.", e);
                }
            }
        } while (response != null);
        //return !responses.isEmpty() ? responses.get(responses.size()-1) : handleFailures(request, new RuntimeException(String.format("Number of retries reached the limit of max retries (configured value %d)", maxRetries)));

        SearchResponse responseToClient = null;
        if (!responses.isEmpty()) {
            responseToClient = responses.get(responses.size() - 1);
        } else {
            responseToClient = handleFailures(request, new RuntimeException(String.format("Number of retries reached the limit of max retries (configured value %d)", maxRetries)));
        }
        return responseToClient;
    }

    private SearchResponse handleRetry(final SearchRequest request, int retryCount, List<SearchResponse> responses) throws InterruptedException {
        try {
            responses.add(openSearchClient.search(request, Object.class));
            return null;
        } catch (Exception e) {
            //incrementErrorCounters(e);
            final boolean doRetry = canRetry(e);
            if (doRetry) {
                return handleFailures(request, e);
            }
        }
        return null;
    }

    public static boolean canRetry(final Exception e) {
        return (e instanceof IOException ||
                (e instanceof OpenSearchException &&
                        !NON_RETRY_STATUS.contains(((OpenSearchException) e).status())));
    }

    private SearchResponse handleRetriesAndFailures(final SearchRequest request,
                                                                                    final int retryCount,
                                                                                    final Exception exceptionFromRequest) {
        final boolean doRetry = Objects.isNull(exceptionFromRequest) || canRetry(exceptionFromRequest);
        if (!doRetry) {
            return handleFailures(request, exceptionFromRequest);
        }
        return null;
    }

    private SearchResponse handleFailures(final SearchRequest request, final Exception exceptionFromRequest) {
        return new SearchResponse.Builder<>()
                .timedOut(false)
                .took(100_000)
                .shards(new ShardStatistics.Builder()
                        .failed(0)
                        .successful(1)
                        .total(1)
                        .build())
                .hits(new HitsMetadata.Builder()
                        .hits(new ArrayList<>())
                        .build())
                .build();
    }

//    private void incrementErrorCounters(final Exception e) {
//        if (e instanceof OpenSearchException) {
//            int status = ((OpenSearchException) e).status();
//            if (NOT_ALLOWED_ERRORS.contains(status)) {
//                bulkRequestNotAllowedErrors.increment();
//            } else if (INVALID_INPUT_ERRORS.contains(status)) {
//                bulkRequestInvalidInputErrors.increment();
//            } else if (NOT_FOUND_ERRORS.contains(status)) {
//                bulkRequestNotFoundErrors.increment();
//            } else if (status == RestStatus.REQUEST_TIMEOUT.getStatus()) {
//                bulkRequestTimeoutErrors.increment();
//            } else if (status >= RestStatus.INTERNAL_SERVER_ERROR.getStatus()) {
//                bulkRequestServerErrors.increment();
//            } else { // Default to Bad Requests
//                bulkRequestBadErrors.increment();
//            }
//        }
//    }
}
