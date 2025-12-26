package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
                "  \"remaining_minutes\": 1440\n" +
                "}";

        DimensionalTimeSliceLeaderProgressState state = objectMapper.readValue(json, DimensionalTimeSliceLeaderProgressState.class);
        assertEquals(Instant.parse("2024-10-20T02:27:15.717Z"), state.getLastPollTime());
        assertEquals(1440, state.getRemainingMinutes());
    }

    @Test
    void testConstructor_setsValuesCorrectly() {
        Instant now = Instant.now();
        long remainingMinutes = 2880;
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, remainingMinutes);

        assertNotNull(state);
        assertEquals(now, state.getLastPollTime());
        assertEquals(2880, state.getRemainingMinutes());
    }

    @Test
    void testConstructor_withSubHourMinutes() {
        Instant now = Instant.now();
        long remainingMinutes = 15;
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, remainingMinutes);

        assertNotNull(state);
        assertEquals(now, state.getLastPollTime());
        assertEquals(15, state.getRemainingMinutes());
    }

    @Test
    void testConstructor_withZeroMinutes() {
        Instant now = Instant.now();
        long remainingMinutes = 0;
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, remainingMinutes);

        assertNotNull(state);
        assertEquals(now, state.getLastPollTime());
        assertEquals(0, state.getRemainingMinutes());
    }

    @Test
    void testBackwardCompatibility_getRemainingHoursFromMinutes() {
        Instant now = Instant.now();
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, 150L);

        assertEquals(150, state.getRemainingMinutes());
        assertEquals(2, state.getRemainingHours());
    }

    @Test
    void testBackwardCompatibility_getRemainingHoursFromSubHourMinutes() {
        Instant now = Instant.now();
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, 30L);

        assertEquals(30, state.getRemainingMinutes());
        assertEquals(0, state.getRemainingHours());
    }

    @Test
    void testBackwardCompatibility_intHoursConstructor() {
        Instant now = Instant.now();
        int hoursFromLegacyConnector = 2;
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, hoursFromLegacyConnector);

        assertNotNull(state);
        assertEquals(now, state.getLastPollTime());
        assertEquals(120, state.getRemainingMinutes());
        assertEquals(2, state.getRemainingHours());
    }

    @Test
    void testBackwardCompatibility_intHoursConstructor_24Hours() {
        Instant now = Instant.now();
        int hoursFromLegacyConnector = 24;
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, hoursFromLegacyConnector);

        assertEquals(1440, state.getRemainingMinutes());
        assertEquals(24, state.getRemainingHours());
    }

    @Test
    void testBackwardCompatibility_intHoursConstructor_zeroHours() {
        Instant now = Instant.now();
        int hoursFromLegacyConnector = 0;
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, hoursFromLegacyConnector);

        assertEquals(0, state.getRemainingMinutes());
        assertEquals(0, state.getRemainingHours());
    }

    @Test
    void testConstructorOverloading_longMinutesVsIntHours() {
        Instant now = Instant.now();

        long minutesValue = 120L;
        DimensionalTimeSliceLeaderProgressState stateFromMinutes = new DimensionalTimeSliceLeaderProgressState(now, minutesValue);
        assertEquals(120, stateFromMinutes.getRemainingMinutes());

        int hoursValue = 2;
        DimensionalTimeSliceLeaderProgressState stateFromHours = new DimensionalTimeSliceLeaderProgressState(now, hoursValue);
        assertEquals(120, stateFromHours.getRemainingMinutes());

        assertEquals(stateFromMinutes.getRemainingMinutes(), stateFromHours.getRemainingMinutes());
    }
}