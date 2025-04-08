package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
        assertInstanceOf(Map.class, result);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals(10, rangeMap.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals(20, rangeMap.get(RangeTypeHandler.LESSER_THAN));
    }

    @Test
    void testHandleInt8Range() {
        when(numericTypeHandler.handle(any(), any(), any())).thenReturn(100L, 200L);
        Object result = rangeTypeHandler.handle(PostgresDataType.INT8RANGE, "test_column", "(100,200]");
        assertInstanceOf(Map.class, result);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals(100L, rangeMap.get(RangeTypeHandler.GREATER_THAN));
        assertEquals(200L, rangeMap.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleNumRange() {
        when(numericTypeHandler.handle(any(), any(), any())).thenReturn("1.0", "2.5");
        Object result = rangeTypeHandler.handle(PostgresDataType.NUMRANGE, "test_column", "(1.0,2.5]");
        assertInstanceOf(Map.class, result);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals("1.0", rangeMap.get(RangeTypeHandler.GREATER_THAN));
        assertEquals("2.5", rangeMap.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleDateRange() {
        when(temporalTypeHandler.handle(any(), any(), any())).thenReturn("1704067200000", "1706659200000");
        Object result = rangeTypeHandler.handle(PostgresDataType.DATERANGE, "test_column", "[2024-01-01,2024-01-31]");
        assertInstanceOf(Map.class, result);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals("1704067200000", rangeMap.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1706659200000", rangeMap.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleTsRange() {
        when(temporalTypeHandler.handle(any(), any(), any())).thenReturn("1704103200000", "1704110400000");
        Object result = rangeTypeHandler.handle(PostgresDataType.TSRANGE, "test_column", "[2024-01-01 10:00:00,2024-01-01 12:00:00)");
        assertInstanceOf(Map.class, result);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals("1704103200000", rangeMap.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1704110400000", rangeMap.get(RangeTypeHandler.LESSER_THAN));
    }

    @Test
    void testHandleTstzRange() {
        when(temporalTypeHandler.handle(any(), any(), any())).thenReturn("1704103200000", "1704110400000");
        Object result = rangeTypeHandler.handle(PostgresDataType.TSTZRANGE, "test_column", "[2024-01-01 10:00:00+00,2024-01-01 12:00:00+00]");
        assertInstanceOf(Map.class, result);
        Map<String, Object> rangeMap = (Map<String, Object>) result;
        assertEquals("1704103200000", rangeMap.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1704110400000", rangeMap.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleInt4RangeArray() {
        when(numericTypeHandler.handle(any(), any(), any())).thenReturn(10, 20, 30, 40);
        Object result = rangeTypeHandler.handle(PostgresDataType.INT4RANGEARRAY, "test_column", "{\"[10,20)\",\"[30,40]\"}");
        assertInstanceOf(List.class, result);
        List<Map<String, Object>> rangeList = (List<Map<String, Object>>) result;

        assertEquals(2, rangeList.size());

        Map<String, Object> range1 = rangeList.get(0);
        assertEquals(10, range1.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals(20, range1.get(RangeTypeHandler.LESSER_THAN));

        Map<String, Object> range2 = rangeList.get(1);
        assertEquals(30, range2.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals(40, range2.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleInt8RangeArray() {
        when(numericTypeHandler.handle(any(), any(), any())).thenReturn(100L, 200L, 300L, 400L);
        Object result = rangeTypeHandler.handle(PostgresDataType.INT8RANGEARRAY, "test_column", "{\"(100,200]\",\"(300,400)\"}");
        assertInstanceOf(List.class, result);
        List<Map<String, Object>> rangeList = (List<Map<String, Object>>) result;

        assertEquals(2, rangeList.size());

        Map<String, Object> range1 = rangeList.get(0);
        assertEquals(100L, range1.get(RangeTypeHandler.GREATER_THAN));
        assertEquals(200L, range1.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));

        Map<String, Object> range2 = rangeList.get(1);
        assertEquals(300L, range2.get(RangeTypeHandler.GREATER_THAN));
        assertEquals(400L, range2.get(RangeTypeHandler.LESSER_THAN));
    }

    @Test
    void testHandleNumRangeArray() {
        when(numericTypeHandler.handle(any(), any(), any())).thenReturn("1.5", "2.0", "3.0", "4.0");
        Object result = rangeTypeHandler.handle(PostgresDataType.NUMRANGEARRAY, "test_column", "{\"[1.5,2.0)\",\"[3.0,4.0]\"}");
        assertInstanceOf(List.class, result);
        List<Map<String, Object>> rangeList = (List<Map<String, Object>>) result;

        assertEquals(2, rangeList.size());

        Map<String, Object> range1 = rangeList.get(0);
        assertEquals("1.5", range1.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("2.0", range1.get(RangeTypeHandler.LESSER_THAN));

        Map<String, Object> range2 = rangeList.get(1);
        assertEquals("3.0", range2.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("4.0", range2.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleDateRangeArray() {
        when(temporalTypeHandler.handle(any(), any(), any())).thenReturn("1704067200000", "1706659200000", "1709337600000", "1711929600000");
        Object result = rangeTypeHandler.handle(PostgresDataType.DATERANGEARRAY, "test_column", "{\"[2024-01-01,2024-01-31]\",\"[2024-03-01,2024-03-31]\"}");
        assertInstanceOf(List.class, result);
        List<Map<String, Object>> rangeList = (List<Map<String, Object>>) result;

        assertEquals(2, rangeList.size());

        Map<String, Object> range1 = rangeList.get(0);
        assertEquals("1704067200000", range1.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1706659200000", range1.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));

        Map<String, Object> range2 = rangeList.get(1);
        assertEquals("1709337600000", range2.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1711929600000", range2.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleTsRangeArray() {
        when(temporalTypeHandler.handle(any(), any(), any())).thenReturn("1704103200000", "1704110400000", "1704189600000", "1704196800000");
        Object result = rangeTypeHandler.handle(PostgresDataType.TSRANGEARRAY, "test_column", "{\"[2024-01-01 10:00:00,2024-01-01 12:00:00)\",\"[2024-01-02 10:00:00,2024-01-02 12:00:00]\"}");
        assertInstanceOf(List.class, result);
        List<Map<String, Object>> rangeList = (List<Map<String, Object>>) result;

        assertEquals(2, rangeList.size());

        Map<String, Object> range1 = rangeList.get(0);
        assertEquals("1704103200000", range1.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1704110400000", range1.get(RangeTypeHandler.LESSER_THAN));

        Map<String, Object> range2 = rangeList.get(1);
        assertEquals("1704189600000", range2.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1704196800000", range2.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleTstzRangeArray() {
        when(temporalTypeHandler.handle(any(), any(), any())).thenReturn("1704103200000", "1704110400000", "1704189600000", "1704196800000");
        Object result = rangeTypeHandler.handle(PostgresDataType.TSTZRANGEARRAY, "test_column", "{\"[2024-01-01 10:00:00+00,2024-01-01 12:00:00+00)\",\"[2024-01-02 10:00:00+00,2024-01-02 12:00:00+00]\"}");
        assertInstanceOf(List.class, result);
        List<Map<String, Object>> rangeList = (List<Map<String, Object>>) result;

        assertEquals(2, rangeList.size());

        Map<String, Object> range1 = rangeList.get(0);
        assertEquals("1704103200000", range1.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1704110400000", range1.get(RangeTypeHandler.LESSER_THAN));

        Map<String, Object> range2 = rangeList.get(1);
        assertEquals("1704189600000", range2.get(RangeTypeHandler.GREATER_THAN_OR_EQUAL_TO));
        assertEquals("1704196800000", range2.get(RangeTypeHandler.LESSER_THAN_OR_EQUAL_TO));
    }

    @Test
    void testHandleEmptyRangeArray() {
        Object result = rangeTypeHandler.handle(PostgresDataType.INT4RANGEARRAY, "test_column", "{}");
        assertInstanceOf(List.class, result);
        List<Map<String, Object>> rangeList = (List<Map<String, Object>>) result;
        assertTrue(rangeList.isEmpty());
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
        assertInstanceOf(Map.class, result);
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

