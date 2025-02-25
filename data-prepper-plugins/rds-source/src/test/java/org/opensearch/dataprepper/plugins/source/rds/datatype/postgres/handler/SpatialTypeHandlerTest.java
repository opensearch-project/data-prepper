package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SpatialTypeHandlerTest {
    private SpatialTypeHandler handler;

    @BeforeEach
    void setUp() {
        handler = new SpatialTypeHandler();
    }

    @Test
    void testHandleNull() {
        assertNull(handler.process(PostgresDataType.POINT, "testColumn", null));
    }

    @Test
    void testHandleNonSpatialType() {
        assertThrows(IllegalArgumentException.class, () ->
                handler.process(PostgresDataType.INTEGER, "testColumn", "(1,2)")
        );
    }

    @ParameterizedTest
    @MethodSource("provideSpatialTypeData")
    void testHandleSpatialTypes(PostgresDataType type, String input, String expected) {
        Object result = handler.process(type, "testColumn", input);
        assertEquals(expected, result);
    }

    @Test
    void testHandleCircle() {
        PostgresDataType dataType = PostgresDataType.CIRCLE;
        Object result = handler.process(dataType, "testColumn", "<(0,0),1>");
        assertTrue(result instanceof Map);
        Map<String, Object> circleMap = (Map<String, Object>) result;
        assertEquals(1.0, circleMap.get("radius"));
        assertTrue(circleMap.get("center") instanceof Map);
        Map<String, Object> centerMap = (Map<String, Object>) circleMap.get("center");
        assertEquals(0.0, centerMap.get("x"));
        assertEquals(0.0, centerMap.get("y"));
    }

    @Test
    void testParseLsegWithInvalidPoints() {
        assertThrows(RuntimeException.class, () ->
                handler.process(PostgresDataType.LSEG, "testColumn", "[(1,1)]")
        );
    }

    @Test
    void testParseBoxWithInvalidPoints() {
        assertThrows(RuntimeException.class, () ->
                handler.process(PostgresDataType.BOX, "testColumn", "(1,1)")
        );
    }

    @Test
    void testParsePathWithNoPoints() {
        assertThrows(RuntimeException.class, () ->
                handler.process(PostgresDataType.PATH, "testColumn", "[]")
        );
    }

    @Test
    void testParseCircleWithNoCenter() {
        assertThrows(RuntimeException.class, () ->
                handler.process(PostgresDataType.CIRCLE, "testColumn", "<>")
        );
    }

    @Test
    void testParsePolygonWithNoPoints() {
        assertThrows(RuntimeException.class, () ->
                handler.process(PostgresDataType.POLYGON, "testColumn", "()")
        );
    }

    private static Stream<Arguments> provideSpatialTypeData() {
        return Stream.of(
                Arguments.of(PostgresDataType.POINT, "(1,2)", "POINT(1.000000 2.000000)" ),
                Arguments.of(PostgresDataType.LINE, "{1,-1,0}", "LINESTRING(0.000000 0.000000, 1.000000 1.000000)" ),
                Arguments.of(PostgresDataType.LINE, "{0,2,3}", "LINESTRING(0.000000 -1.500000, 1.000000 -1.500000)" ),
                Arguments.of(PostgresDataType.LINE, "{2,0,3}", "LINESTRING(-1.500000 0.000000, -1.500000 1.000000)" ),

                Arguments.of(PostgresDataType.LSEG, "[(1,1),(2,2)]", "LINESTRING(1.000000 1.000000, 2.000000 2.000000)"),
                Arguments.of(PostgresDataType.BOX, "(1,1),(2,2)", "POLYGON((1.000000 1.000000, 2.000000 1.000000, 2.000000 2.000000, 1.000000 2.000000, 1.000000 1.000000))" ),
                Arguments.of(PostgresDataType.PATH, "[(1,1),(2,2),(3,3)]", "LINESTRING(1.000000 1.000000, 2.000000 2.000000, 3.000000 3.000000)"),
                Arguments.of(PostgresDataType.PATH, "((1,1),(2,2),(3,3))", "POLYGON((1.000000 1.000000, 2.000000 2.000000, 3.000000 3.000000, 1.000000 1.000000))"),
                Arguments.of(PostgresDataType.POLYGON, "((0,0),(0,1),(1,1),(1,0))", "POLYGON((0.000000 0.000000, 0.000000 1.000000, 1.000000 1.000000, 1.000000 0.000000, 0.000000 0.000000))")
        );
    }

}
