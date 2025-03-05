package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class RangeTypeHandlerTest {

    @Mock
    private NumericTypeHandler numericTypeHandler;

    @Mock
    private TemporalTypeHandler temporalTypeHandler;

    private RangeTypeHandler rangeTypeHandler;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        rangeTypeHandler = new RangeTypeHandler(numericTypeHandler, temporalTypeHandler);
    }

    @Test
    void testHandleInt4Range() {
        when(numericTypeHandler.handle(any(), any(), any())).thenReturn(10, 20);
        Object result = rangeTypeHandler.handle(PostgresDataType.INT4RANGE, "test_column", "[10,20)");
        assertTrue(result instanceof Map);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals(10, rangeMap.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals(20, rangeMap.get(RangeTypeHandler.LESSER_THAN));
    }

    @Test
    void testHandleInt8Range() {
        when(numericTypeHandler.handle(any(), any(), any())).thenReturn(100L, 200L);
        Object result = rangeTypeHandler.handle(PostgresDataType.INT8RANGE, "test_column", "(100,200]");
        assertTrue(result instanceof Map);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals(100L, rangeMap.get(RangeTypeHandler.GREATER_THAN));
        assertEquals(200L, rangeMap.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleDateRange() {
        when(temporalTypeHandler.handle(any(), any(), any())).thenReturn("1704067200000", "1706659200000");
        Object result = rangeTypeHandler.handle(PostgresDataType.DATERANGE, "test_column", "[2024-01-01,2024-01-31]");
        assertTrue(result instanceof Map);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals("1704067200000", rangeMap.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1706659200000", rangeMap.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleTsRange() {
        when(temporalTypeHandler.handle(any(), any(), any())).thenReturn("1704103200000", "1704110400000");
        Object result = rangeTypeHandler.handle(PostgresDataType.TSRANGE, "test_column", "[2024-01-01 10:00:00,2024-01-01 12:00:00)");
        assertTrue(result instanceof Map);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals("1704103200000", rangeMap.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1704110400000", rangeMap.get(RangeTypeHandler.LESSER_THAN));
    }

    @Test
    void testHandleTstzRange() {
        when(temporalTypeHandler.handle(any(), any(), any())).thenReturn("1704103200000", "1704110400000");
        Object result = rangeTypeHandler.handle(PostgresDataType.TSTZRANGE, "test_column", "[2024-01-01 10:00:00+00,2024-01-01 12:00:00+00]");
        assertTrue(result instanceof Map);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals("1704103200000", rangeMap.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1704110400000", rangeMap.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleEmptyRange() {
        Object result = rangeTypeHandler.handle(PostgresDataType.INT4RANGE, "test_column", RangeTypeHandler.EMPTY);
        assertNull(result);
    }

    @Test
    void testHandleInfiniteRange() {
        when(numericTypeHandler.handle(any(), any(), any())).thenReturn(10);
        Object result = rangeTypeHandler.handle(PostgresDataType.INT4RANGE, "test_column", "[10,)");
        assertTrue(result instanceof Map);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals(10, rangeMap.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertFalse(rangeMap.containsKey(RangeTypeHandler.LESSER_THAN));
        assertFalse(rangeMap.containsKey(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleInvalidRangeFormat() {
        assertThrows(IllegalArgumentException.class, () ->
                rangeTypeHandler.handle(PostgresDataType.INT4RANGE, "test_column", "1020")
        );
    }

    @Test
    void testHandleNonRangeType() {
        assertThrows(IllegalArgumentException.class, () ->
                rangeTypeHandler.handle(PostgresDataType.INTEGER, "test_column", "[10,20)")
        );
    }
}

