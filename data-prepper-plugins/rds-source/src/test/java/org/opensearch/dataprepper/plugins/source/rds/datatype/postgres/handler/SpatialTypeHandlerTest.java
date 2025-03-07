package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class SpatialTypeHandlerTest {
    private SpatialTypeHandler handler;

    private static Stream<Arguments> provideSpatialArrayTypeData() {
        return Stream.of(
                Arguments.of(PostgresDataType.POINTARRAY,
                        "{\"(1,2)\",\"(3,4)\"}",
                        Arrays.asList("POINT(1.000000 2.000000)", "POINT(3.000000 4.000000)")),

                Arguments.of(PostgresDataType.LINEARRAY,
                        "{\"{1,-1,0}\",\"{0,2,3}\"}",
                        Arrays.asList("LINESTRING(0.000000 0.000000, 1.000000 1.000000)",
                                "LINESTRING(0.000000 -1.500000, 1.000000 -1.500000)")),

                Arguments.of(PostgresDataType.LSEGARRAY,
                        "{\"[(1,1),(2,2)]\",\"[(3,3),(4,4)]\"}",
                        Arrays.asList("LINESTRING(1.000000 1.000000, 2.000000 2.000000)",
                                "LINESTRING(3.000000 3.000000, 4.000000 4.000000)")),

                Arguments.of(PostgresDataType.BOXARRAY,
                        "{\"(1,1),(2,2)\";\"(3,3),(4,4)\"}",
                        Arrays.asList("POLYGON((1.000000 1.000000, 2.000000 1.000000, 2.000000 2.000000, 1.000000 2.000000, 1.000000 1.000000))",
                                "POLYGON((3.000000 3.000000, 4.000000 3.000000, 4.000000 4.000000, 3.000000 4.000000, 3.000000 3.000000))")),

                Arguments.of(PostgresDataType.PATHARRAY,
                        "{\"[(1,1),(2,2),(3,3)]\",\"((4,4),(5,5),(6,6))\"}",
                        Arrays.asList("LINESTRING(1.000000 1.000000, 2.000000 2.000000, 3.000000 3.000000)",
                                "POLYGON((4.000000 4.000000, 5.000000 5.000000, 6.000000 6.000000, 4.000000 4.000000))")),

                Arguments.of(PostgresDataType.POLYGONARRAY,
                        "{\"((0,0),(0,1),(1,1),(1,0))\",\"((2,2),(2,3),(3,3),(3,2))\"}",
                        Arrays.asList("POLYGON((0.000000 0.000000, 0.000000 1.000000, 1.000000 1.000000, 1.000000 0.000000, 0.000000 0.000000))",
                                "POLYGON((2.000000 2.000000, 2.000000 3.000000, 3.000000 3.000000, 3.000000 2.000000, 2.000000 2.000000))"))

        );
    }

    private static Stream<Arguments> provideSpatialTypeData() {
        return Stream.of(
                Arguments.of(PostgresDataType.POINT, "(1,2)", "POINT(1.000000 2.000000)"),
                Arguments.of(PostgresDataType.LINE, "{1,-1,0}", "LINESTRING(0.000000 0.000000, 1.000000 1.000000)"),
                Arguments.of(PostgresDataType.LINE, "{0,2,3}", "LINESTRING(0.000000 -1.500000, 1.000000 -1.500000)"),
                Arguments.of(PostgresDataType.LINE, "{2,0,3}", "LINESTRING(-1.500000 0.000000, -1.500000 1.000000)"),

                Arguments.of(PostgresDataType.LSEG, "[(1,1),(2,2)]", "LINESTRING(1.000000 1.000000, 2.000000 2.000000)"),
                Arguments.of(PostgresDataType.BOX, "(1,1),(2,2)", "POLYGON((1.000000 1.000000, 2.000000 1.000000, 2.000000 2.000000, 1.000000 2.000000, 1.000000 1.000000))"),
                Arguments.of(PostgresDataType.PATH, "[(1,1),(2,2),(3,3)]", "LINESTRING(1.000000 1.000000, 2.000000 2.000000, 3.000000 3.000000)"),
                Arguments.of(PostgresDataType.PATH, "((1,1),(2,2),(3,3))", "POLYGON((1.000000 1.000000, 2.000000 2.000000, 3.000000 3.000000, 1.000000 1.000000))"),
                Arguments.of(PostgresDataType.POLYGON, "((0,0),(0,1),(1,1),(1,0))", "POLYGON((0.000000 0.000000, 0.000000 1.000000, 1.000000 1.000000, 1.000000 0.000000, 0.000000 0.000000))")
        );
    }

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
        assertInstanceOf(Map.class, result);
        Map<String, Object> circleMap = (Map<String, Object>) result;
        assertEquals(1.0, circleMap.get("radius"));
        assertInstanceOf(Map.class, circleMap.get("center"));
        Map<String, Object> centerMap = (Map<String, Object>) circleMap.get("center");
        assertEquals(0.0, centerMap.get("x"));
        assertEquals(0.0, centerMap.get("y"));
    }

    @ParameterizedTest
    @MethodSource("provideSpatialArrayTypeData")
    void testHandleSpatialArrayTypes(PostgresDataType type, String input, List<String> expected) {
        Object result = handler.process(type, "testColumn", input);
        assertThat(result, instanceOf(List.class));
        assertEquals(expected, result);
    }

    @Test
    void testHandleNullSpatialArray() {
        assertNull(handler.process(PostgresDataType.POINTARRAY, "testColumn", null));
    }

    @Test
    void testHandleEmptySpatialArray() {
        Object result = handler.process(PostgresDataType.POINTARRAY, "testColumn", "{}");
        assertThat(result, instanceOf(List.class));
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    void testHandleCircleArray() {
        Object result = handler.process(PostgresDataType.CIRCLEARRAY, "testColumn", "{\"<(0,0),1>\",\"<(2,2),3>\"}");
        assertThat(result, instanceOf(List.class));
        List<Map<String, Object>> circleList = (List<Map<String, Object>>) result;
        assertEquals(2, circleList.size());

        Map<String, Object> circle1 = circleList.get(0);
        assertEquals(1.0, circle1.get("radius"));
        Map<String, Object> center1 = (Map<String, Object>) circle1.get("center");
        assertEquals(0.0, center1.get("x"));
        assertEquals(0.0, center1.get("y"));

        Map<String, Object> circle2 = circleList.get(1);
        assertEquals(3.0, circle2.get("radius"));
        Map<String, Object> center2 = (Map<String, Object>) circle2.get("center");
        assertEquals(2.0, center2.get("x"));
        assertEquals(2.0, center2.get("y"));
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

}
