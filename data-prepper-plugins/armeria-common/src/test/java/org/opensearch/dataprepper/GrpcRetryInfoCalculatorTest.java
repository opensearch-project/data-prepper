package org.opensearch.dataprepper;

import com.google.rpc.RetryInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class GrpcRetryInfoCalculatorTest {

    @Test
    public void testMinimumDelayOnFirstCall() {
        RetryInfo retryInfo = new GrpcRetryInfoCalculator(Duration.ofMillis(100), Duration.ofSeconds(1)).createRetryInfo();

        assertThat(retryInfo.getRetryDelay().getNanos(), equalTo(100_000_000));
        assertThat(retryInfo.getRetryDelay().getSeconds(), equalTo(0L));
    }

    @Test
    public void testExponentialBackoff() {
        GrpcRetryInfoCalculator calculator =
                new GrpcRetryInfoCalculator(Duration.ofSeconds(1), Duration.ofSeconds(10));
        RetryInfo retryInfo1 = calculator.createRetryInfo();
        RetryInfo retryInfo2 = calculator.createRetryInfo();
        RetryInfo retryInfo3 = calculator.createRetryInfo();
        RetryInfo retryInfo4 = calculator.createRetryInfo();

        assertThat(retryInfo1.getRetryDelay().getSeconds(), equalTo(1L));
        assertThat(retryInfo2.getRetryDelay().getSeconds(), equalTo(1L));
        assertThat(retryInfo3.getRetryDelay().getSeconds(), equalTo(2L));
        assertThat(retryInfo4.getRetryDelay().getSeconds(), equalTo(4L));
    }

    @Test
    public void testUsesMaximumAsLongestDelay() {
        GrpcRetryInfoCalculator calculator =
                new GrpcRetryInfoCalculator(Duration.ofSeconds(1), Duration.ofSeconds(2));
        RetryInfo retryInfo1 = calculator.createRetryInfo();
        RetryInfo retryInfo2 = calculator.createRetryInfo();
        RetryInfo retryInfo3 = calculator.createRetryInfo();

        assertThat(retryInfo1.getRetryDelay().getSeconds(), equalTo(1L));
        assertThat(retryInfo2.getRetryDelay().getSeconds(), equalTo(1L));
        assertThat(retryInfo3.getRetryDelay().getSeconds(), equalTo(2L));
    }

    @Test
    public void testResetAfterDelayWearsOff() throws InterruptedException {
        int minDelayNanos = 1_000_000;
        GrpcRetryInfoCalculator calculator =
                new GrpcRetryInfoCalculator(Duration.ofNanos(minDelayNanos), Duration.ofSeconds(1));

        RetryInfo retryInfo1 = calculator.createRetryInfo();
        RetryInfo retryInfo2 = calculator.createRetryInfo();
        RetryInfo retryInfo3 = calculator.createRetryInfo();
        sleep(retryInfo3);
        RetryInfo retryInfo4 = calculator.createRetryInfo();

        assertThat(retryInfo1.getRetryDelay().getNanos(), equalTo(minDelayNanos));
        assertThat(retryInfo2.getRetryDelay().getNanos(), equalTo(minDelayNanos));
        assertThat(retryInfo3.getRetryDelay().getNanos(), equalTo(minDelayNanos * 2));
        assertThat(retryInfo4.getRetryDelay().getNanos(), equalTo(minDelayNanos));
    }

    @Test
    public void testQuickFirstExceptionDoesNotTriggerBackoffCalculationEvenWithLongMinDelay() throws InterruptedException {
        GrpcRetryInfoCalculator calculator =
                new GrpcRetryInfoCalculator(Duration.ofSeconds(10), Duration.ofSeconds(20));

        RetryInfo retryInfo1 = calculator.createRetryInfo();
        RetryInfo retryInfo2 = calculator.createRetryInfo();

        assertThat(retryInfo1.getRetryDelay().getSeconds(), equalTo(10L));
        assertThat(retryInfo2.getRetryDelay().getSeconds(), equalTo(10L));
    }

    private void sleep(RetryInfo retryInfo) throws InterruptedException {
        // make sure we let enough time pass by adding a few milliseconds on top
        Thread.sleep((retryInfo.getRetryDelay().getNanos() / 1_000_000) + 200 );
    }
}
