package com.amazon.situp.pipeline.common;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureHelper {
    private static final Logger LOG = LoggerFactory.getLogger(FutureHelper.class);

    /**
     * Helper method to handle the return of multiple futures. The method will return
     * as soon as all the futures are complete or until the specified timeout duration.
     * @param futureList
     * @param duration
     * @param timeUnit
     * @param stopwatch
     * @param <A>
     * @return A list of completed, errored, and uncompleted future tasks.
     */
    public static <A> FutureHelperResult<A> awaitFutures(
            final List<Future<A>> futureList,
            long duration, TimeUnit timeUnit,
            final Stopwatch stopwatch) {
        final List<A> completedFutureResults = new ArrayList<>(futureList.size());
        final List<Future<A>> uncompletedFutures = new ArrayList<>();
        final List<Integer> uncompletedIndexes = new ArrayList<>();
        final List<ExecutionException> failedExceptionList = new ArrayList<>();
        final List<Integer> failedIndexes = new ArrayList<>();

        final Queue<Future<A>> futureQueue = new LinkedList<>(futureList);
        long totalMillisRemaining = timeUnit.toMillis(duration);

        int index = -1;
        while (!futureQueue.isEmpty()) {
            stopwatch.reset();
            stopwatch.start();
            final Future<A> firstFuture = futureQueue.remove();
            index++;
            try {
                if (totalMillisRemaining > 0) {

                    final A value = firstFuture.get(totalMillisRemaining, TimeUnit.MILLISECONDS);
                    completedFutureResults.add(value);

                } else {
                    if (firstFuture.isDone()) {
                        //at this point the future should return immediately
                        completedFutureResults.add(firstFuture.get(1, TimeUnit.MILLISECONDS));
                    } else {
                        uncompletedFutures.add(firstFuture);
                        uncompletedIndexes.add(index);
                    }
                }
            } catch (ExecutionException e) {
                //There is some problem with the request
                failedExceptionList.add(e);
                failedIndexes.add(index);
                LOG.error("FutureTask failed due to: ", e);
            } catch (InterruptedException | TimeoutException t) {
                //Out of time
                uncompletedFutures.add(firstFuture);
                uncompletedIndexes.add(index);
            }
            totalMillisRemaining -= stopwatch.elapsed(TimeUnit.MILLISECONDS);

        }

        return new FutureHelperResult<>(completedFutureResults, uncompletedFutures, uncompletedIndexes
                ,failedExceptionList, failedIndexes);

    }

    public static <A> FutureHelperResult<A> awaitFuturesIndefinitely(final List<Future<A>> futureList) {
        final List<A> completedFutureResults = new ArrayList<>(futureList.size());
        final List<ExecutionException> failedExceptionList = new ArrayList<>();

        final Queue<Future<A>> futureQueue = new LinkedList<>(futureList);
        while(!futureQueue.isEmpty()) {
            final Future<A> future = futureQueue.remove();
            try{
                if(future.isDone()) {
                    completedFutureResults.add(future.get(1, TimeUnit.MILLISECONDS));
                } else {
                    futureQueue.add(future); //add it back to queue
                }
            } catch (ExecutionException e) {
                failedExceptionList.add(e);
                LOG.error("FutureTask failed due to: ", e);
            } catch (InterruptedException | TimeoutException e) {
                LOG.error("FutureTask is interrupted or timed out");
            }
        }
        return new FutureHelperResult<>(completedFutureResults, failedExceptionList);
    }
}
