package org.opensearch.dataprepper.model.acknowledgements;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ExpiryItemTest {
    private static final String ITEM_ID = UUID.randomUUID().toString();
    private static final long POLL_SECONDS = 27;

    private Instant expirationTime = Instant.now();
    @Mock
    private Consumer<ExpiryItem> expiryCallback;
    @Mock
    private AcknowledgementSet acknowledgementSet;

    public ExpiryItem expiryItem;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);

        this.expiryItem = new ExpiryItem(ITEM_ID, POLL_SECONDS, expirationTime, expiryCallback, acknowledgementSet);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(expiryCallback, acknowledgementSet);
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 0, 1, 2})
    void pollDelayTooLow_ThrowsUnsupportedOperationException(final long pollSeconds) {
        assertThrows(UnsupportedOperationException.class,
                () -> new ExpiryItem(ITEM_ID, pollSeconds, expirationTime, expiryCallback, acknowledgementSet));
    }

    @Test
    void executeExpiryCallback_Success() {
        doNothing().when(expiryCallback).accept(expiryItem);

        final boolean result = expiryItem.executeExpiryCallback();
        assertThat(result, equalTo(true));

        verify(expiryCallback).accept(expiryItem);
    }

    @Test
    void executeExpiryCallback_Failure() {
        doThrow(new RuntimeException()).when(expiryCallback).accept(expiryItem);

        final boolean result = expiryItem.executeExpiryCallback();
        assertThat(result, equalTo(false));

        verify(expiryCallback).accept(expiryItem);
    }

    @Test
    void isCompleteOrExpired_false() {
        when(acknowledgementSet.isDone()).thenReturn(false);
        expiryItem.setExpirationTime(expirationTime.plusSeconds(60));

        final boolean result = expiryItem.isCompleteOrExpired();
        assertThat(result, equalTo(false));

        verify(acknowledgementSet).isDone();
    }

    @Test
    void isCompleteOrExpired_AckSetDone() {
        when(acknowledgementSet.isDone()).thenReturn(true);
        expiryItem.setExpirationTime(expirationTime.plusSeconds(60));

        final boolean result = expiryItem.isCompleteOrExpired();
        assertThat(result, equalTo(true));

        verify(acknowledgementSet).isDone();
    }

    @Test
    void isCompleteOrExpired_Expired() {
        expiryItem.setExpirationTime(expirationTime.minusSeconds(60));

        final boolean result = expiryItem.isCompleteOrExpired();
        assertThat(result, equalTo(true));
    }

    @Test
    void updateExpirationTime_Success() {
        final Instant expectedUpdatedExpiryTime = expirationTime.plusSeconds(POLL_SECONDS);

        expiryItem.updateExpirationTime();

        verify(acknowledgementSet).setExpiryTime(expectedUpdatedExpiryTime);
    }

    @Test
    void getItemId_Success() {
        assertThat(expiryItem.getItemId(), equalTo(ITEM_ID));
    }

    @Test
    void getPollSeconds_Success() {
        assertThat(expiryItem.getPollSeconds(), equalTo(POLL_SECONDS));
    }
}
