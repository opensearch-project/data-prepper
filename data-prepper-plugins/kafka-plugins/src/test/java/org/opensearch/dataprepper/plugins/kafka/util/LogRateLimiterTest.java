package org.opensearch.dataprepper.plugins.kafka.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
public class LogRateLimiterTest {

    @Test
    public void testRateLimiter() {
        long currentMs = System.currentTimeMillis();
        LogRateLimiter objUnderTest = new LogRateLimiter(10, currentMs);
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(true));
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(false));
        currentMs += 50;
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(false));
        currentMs += 50;
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(true));
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(false));
        currentMs += 876;
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(true));
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(false));

        currentMs = System.currentTimeMillis();
        objUnderTest = new LogRateLimiter(2, currentMs);
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(true));
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(false));
        currentMs += 100;
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(false));
        currentMs += 200;
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(false));
        currentMs += 500;
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(true));

        currentMs = System.nanoTime();
        objUnderTest = new LogRateLimiter(1000, currentMs);
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(true));
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(false));
        currentMs += 1;
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(true));
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(false));
        currentMs += 2;
        assertThat(objUnderTest.isAllowed(currentMs), equalTo(true));
    }

    @Test
    public void testRateLimiterInvalidArgs() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LogRateLimiter(1345, System.currentTimeMillis())
        );
    }
}