package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DimensionalTimeSliceLeaderProgressStateTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Test
    void testDeserializeDimensionalTimeSliceLeaderProgressState_withTypeInfo() throws JsonProcessingException {
        String json = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceLeaderProgressState\",\n" +
                "  \"last_poll_time\": \"2024-10-20T02:27:15.717Z\",\n" +
                "  \"remaining_duration\": \"2024-10-19T02:27:15.717Z\"\n" +
                "}";

        DimensionalTimeSliceLeaderProgressState state = objectMapper.readValue(json, DimensionalTimeSliceLeaderProgressState.class);
        assertEquals(Instant.parse("2024-10-20T02:27:15.717Z"), state.getLastPollTime());
        long remainingMinutes = Duration.between(state.getRemainingDuration(), state.getLastPollTime()).toMinutes();
        assertEquals(1440, remainingMinutes);
    }

    @Test
    void testConstructor_setsValuesCorrectly() {
        Instant now = Instant.now();
        Instant remainingDuration = now.minus(Duration.ofMinutes(2880));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, remainingDuration);

        assertNotNull(state);
        assertEquals(now, state.getLastPollTime());
        assertEquals(remainingDuration, state.getRemainingDuration());
        long remainingMinutes = Duration.between(state.getRemainingDuration(), state.getLastPollTime()).toMinutes();
        assertEquals(2880, remainingMinutes);
    }

    @Test
    void testConstructor_withSubHourMinutes() {
        Instant now = Instant.now();
        Instant remainingDuration = now.minus(Duration.ofMinutes(15));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, remainingDuration);

        assertNotNull(state);
        assertEquals(now, state.getLastPollTime());
        assertEquals(remainingDuration, state.getRemainingDuration());
        long remainingMinutes = Duration.between(state.getRemainingDuration(), state.getLastPollTime()).toMinutes();
        assertEquals(15, remainingMinutes);
    }

    @Test
    void testConstructor_withZeroMinutes() {
        Instant now = Instant.now();
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, now);

        assertNotNull(state);
        assertEquals(now, state.getLastPollTime());
        assertEquals(now, state.getRemainingDuration());
        long remainingMinutes = Duration.between(state.getRemainingDuration(), state.getLastPollTime()).toMinutes();
        assertEquals(0, remainingMinutes);
    }

    @Test
    void testBackwardCompatibility_getRemainingHoursFromMinutes() {
        Instant now = Instant.now();
        Instant remainingDuration = now.minus(Duration.ofMinutes(150));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, remainingDuration);

        long remainingMinutes = Duration.between(state.getRemainingDuration(), state.getLastPollTime()).toMinutes();
        assertEquals(150, remainingMinutes);
        assertEquals(2, state.getRemainingHours());
    }

    @Test
    void testBackwardCompatibility_getRemainingHoursFromSubHourMinutes() {
        Instant now = Instant.now();
        Instant remainingDuration = now.minus(Duration.ofMinutes(30));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, remainingDuration);

        long remainingMinutes = Duration.between(state.getRemainingDuration(), state.getLastPollTime()).toMinutes();
        assertEquals(30, remainingMinutes);
        assertEquals(0, state.getRemainingHours());
    }

    @Test
    void testBackwardCompatibility_intHoursConstructor() {
        Instant now = Instant.now();
        int hoursFromLegacyConnector = 2;
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, hoursFromLegacyConnector);

        assertNotNull(state);
        assertEquals(now, state.getLastPollTime());
        long remainingMinutes = Duration.between(state.getRemainingDuration(), state.getLastPollTime()).toMinutes();
        assertEquals(120, remainingMinutes);
        assertEquals(2, state.getRemainingHours());
    }

    @Test
    void testBackwardCompatibility_intHoursConstructor_24Hours() {
        Instant now = Instant.now();
        int hoursFromLegacyConnector = 24;
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, hoursFromLegacyConnector);

        long remainingMinutes = Duration.between(state.getRemainingDuration(), state.getLastPollTime()).toMinutes();
        assertEquals(1440, remainingMinutes);
        assertEquals(24, state.getRemainingHours());
    }

    @Test
    void testBackwardCompatibility_intHoursConstructor_zeroHours() {
        Instant now = Instant.now();
        int hoursFromLegacyConnector = 0;
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, hoursFromLegacyConnector);

        long remainingMinutes = Duration.between(state.getRemainingDuration(), state.getLastPollTime()).toMinutes();
        assertEquals(0, remainingMinutes);
        assertEquals(0, state.getRemainingHours());
    }

    @Test
    void testConstructorOverloading_longMinutesVsIntHours() {
        Instant now = Instant.now();

        Instant remainingDurationFromMinutes = now.minus(Duration.ofMinutes(120));
        DimensionalTimeSliceLeaderProgressState stateFromMinutes = new DimensionalTimeSliceLeaderProgressState(now, remainingDurationFromMinutes);
        long remainingMinutesFromMinutes = Duration.between(stateFromMinutes.getRemainingDuration(), stateFromMinutes.getLastPollTime()).toMinutes();
        assertEquals(120, remainingMinutesFromMinutes);

        int hoursValue = 2;
        DimensionalTimeSliceLeaderProgressState stateFromHours = new DimensionalTimeSliceLeaderProgressState(now, hoursValue);
        long remainingMinutesFromHours = Duration.between(stateFromHours.getRemainingDuration(), stateFromHours.getLastPollTime()).toMinutes();
        assertEquals(120, remainingMinutesFromHours);

        assertEquals(remainingMinutesFromMinutes, remainingMinutesFromHours);
    }
}