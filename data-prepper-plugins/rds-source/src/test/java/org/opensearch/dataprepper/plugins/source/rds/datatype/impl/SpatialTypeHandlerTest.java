package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SpatialTypeHandlerTest {

    @Test
    public void test_handleInvalidType() {
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                List.of("invalid_col"), List.of("invalid_col"));
        final DataTypeHandler spatialTypeHandler = new SpatialTypeHandler();

        assertThrows(IllegalArgumentException.class, () -> {
            spatialTypeHandler.handle(MySQLDataType.GEOMETRY, "invalid_col", "not_a_geometry", metadata);
        });
    }

    @Test
    public void test_handleInvalidGeometryValue() {
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                List.of("invalid_col"), List.of("invalid_col"));
        final DataTypeHandler spatialTypeHandler = new SpatialTypeHandler();

        assertThrows(RuntimeException.class, () -> {
            spatialTypeHandler.handle(MySQLDataType.GEOMETRY, "invalid_col", "not_a_geometry".getBytes(), metadata);
        });
    }

    @ParameterizedTest
    @MethodSource("provideGeometryTypeData")
    public void test_handleGeometryTypes_success(final MySQLDataType mySQLDataType, final String columnName, final Object value, final Object expectedValue) {
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName));
        final DataTypeHandler numericTypeHandler = new SpatialTypeHandler();
        Object result = numericTypeHandler.handle(mySQLDataType, columnName, value, metadata);

        if (result != null) {
            assertThat(result, instanceOf(expectedValue.getClass()));
        }
        assertThat(result, is(expectedValue));
    }

    private static Stream<Arguments> provideGeometryTypeData() {
        return Stream.of(
                Arguments.of(MySQLDataType.GEOMETRY, "point_col", Map.of("bytes", createGeometryString("Point", new double[]{1.0, 1.0})),
                        "POINT(1.000000 1.000000)"),
                Arguments.of(MySQLDataType.GEOMETRY, "point_col", null, null),
                Arguments.of(MySQLDataType.GEOMETRY, "point_col", Collections.singletonMap("bytes", null), null),
                Arguments.of(MySQLDataType.GEOMETRY, "point_col", createGeometryBytes("Point", new double[]{1.0, 1.0}),
                        "POINT(1.000000 1.000000)"),
                Arguments.of(MySQLDataType.GEOMETRY, "linestring_col", Map.of("bytes", createGeometryString("LineString", new double[][]{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}})),
                        "LINESTRING(1.000000 1.000000, 2.000000 2.000000, 3.000000 3.000000)"),
                Arguments.of(MySQLDataType.GEOMETRY, "polygon_col", Map.of("bytes", createGeometryString("Polygon", new double[][][]{{{0.0, 0.0}, {0.0, 4.0}, {4.0, 4.0}, {4.0, 0.0}, {0.0, 0.0}}})),
                        "POLYGON((0.000000 0.000000, 0.000000 4.000000, 4.000000 4.000000, 4.000000 0.000000, 0.000000 0.000000))"),
                Arguments.of(MySQLDataType.GEOMETRY, "multipoint_col", Map.of("bytes", createGeometryString("MultiPoint", new double[][]{{1.0, 1.0}, {2.0, 2.0}})),
                        "MULTIPOINT(1.000000 1.000000, 2.000000 2.000000)"),
                Arguments.of(MySQLDataType.GEOMETRY, "multilinestring_col", Map.of("bytes", createGeometryString("MultiLineString", new double[][][]{{{1.0, 1.0}, {2.0, 2.0}}, {{3.0, 3.0}, {4.0, 4.0}}})),
                        "MULTILINESTRING((1.000000 1.000000, 2.000000 2.000000), (3.000000 3.000000, 4.000000 4.000000))"),
                Arguments.of(MySQLDataType.GEOMETRY, "multipolygon_col", Map.of("bytes", createGeometryString("MultiPolygon", new double[][][][]{{{{0.0, 0.0}, {0.0, 4.0}, {4.0, 4.0}, {4.0, 0.0}, {0.0, 0.0}}}, {{{5.0, 5.0}, {5.0, 7.0}, {7.0, 7.0}, {7.0, 5.0}, {5.0, 5.0}}}})),
                        "MULTIPOLYGON(((0.000000 0.000000, 0.000000 4.000000, 4.000000 4.000000, 4.000000 0.000000, 0.000000 0.000000)), ((5.000000 5.000000, 5.000000 7.000000, 7.000000 7.000000, 7.000000 5.000000, 5.000000 5.000000)))"),
                Arguments.of(MySQLDataType.GEOMETRY, "geometrycollection_col", Map.of("bytes", createGeometryString("GeometryCollection", List.of(
                                new Object[]{"Point", new double[]{1.0, 1.0}},
                                new Object[]{"LineString", new double[][]{{2.0, 2.0}, {3.0, 3.0}}}
                        ))),
                        "GEOMETRYCOLLECTION(POINT(1.000000 1.000000), LINESTRING(2.000000 2.000000, 3.000000 3.000000))"),
                Arguments.of(MySQLDataType.GEOMETRY, "geometrycollection_col", createGeometryBytes("GeometryCollection", List.of(
                                new Object[]{"Point", new double[]{1.0, 1.0}},
                                new Object[]{"LineString", new double[][]{{2.0, 2.0}, {3.0, 3.0}}}
                        )),
                        "GEOMETRYCOLLECTION(POINT(1.000000 1.000000), LINESTRING(2.000000 2.000000, 3.000000 3.000000))")
        );
    }

    public static ByteBuffer createGeometryBuffer(String type, Object data) {
        ByteBuffer buffer;
        switch (type) {
            case "Point":
                buffer = createPoint((double[]) data);
                break;
            case "LineString":
                buffer = createLineString((double[][]) data);
                break;
            case "Polygon":
                buffer = createPolygon((double[][][]) data);
                break;
            case "MultiPoint":
                buffer = createMultiPoint((double[][]) data);
                break;
            case "MultiLineString":
                buffer = createMultiLineString((double[][][]) data);
                break;
            case "MultiPolygon":
                buffer = createMultiPolygon((double[][][][]) data);
                break;
            case "GeometryCollection":
                buffer = createGeometryCollection((List<Object[]>) data);
                break;
            default:
                throw new IllegalArgumentException("Unsupported geometry type: " + type);
        }

        return buffer;
    }

    public static String createGeometryString(String type, Object data) {
        final ByteBuffer buffer = createGeometryBuffer(type, data);
        StringBuilder sb = new StringBuilder();
        for (byte b : buffer.array()) {
            sb.append((char) (b & 0xFF));
        }
        return sb.toString();
    }

    public static byte[] createGeometryBytes(String type, Object data) {
        final ByteBuffer buffer = createGeometryBuffer(type, data);
        return buffer.array();
    }

    private static ByteBuffer createPoint(double[] coordinates) {
        ByteBuffer buffer = ByteBuffer.allocate(25); // 4 (SRID) + 1 (byte order) + 4 (type) + 16 (2 doubles)
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0); // SRID
        buffer.put((byte) 1); // Little endian
        buffer.putInt(1); // Point type
        buffer.putDouble(coordinates[0]);
        buffer.putDouble(coordinates[1]);
        return buffer;
    }

    private static ByteBuffer createLineString(double[][] points) {
        ByteBuffer buffer = ByteBuffer.allocate(13 + 16 * points.length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0); // SRID
        buffer.put((byte) 1); // Little endian
        buffer.putInt(2); // LineString type
        buffer.putInt(points.length);
        for (double[] point : points) {
            buffer.putDouble(point[0]);
            buffer.putDouble(point[1]);
        }
        return buffer;
    }

    private static ByteBuffer createPolygon(double[][][] rings) {
        int size = 13; // 4 (SRID) + 1 (byte order) + 4 (type) + 4 (num rings)
        for (double[][] ring : rings) {
            size += 4 + 16 * ring.length; // 4 (num points) + 16 (2 doubles) * num points
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0); // SRID
        buffer.put((byte) 1); // Little endian
        buffer.putInt(3); // Polygon type
        buffer.putInt(rings.length);
        for (double[][] ring : rings) {
            buffer.putInt(ring.length);
            for (double[] point : ring) {
                buffer.putDouble(point[0]);
                buffer.putDouble(point[1]);
            }
        }
        return buffer;
    }

    private static ByteBuffer createMultiPoint(double[][] points) {
        ByteBuffer buffer = ByteBuffer.allocate(13 + 21 * points.length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0); // SRID
        buffer.put((byte) 1); // Little endian
        buffer.putInt(4); // MultiPoint type
        buffer.putInt(points.length);
        for (double[] point : points) {
            buffer.put((byte) 1); // Little endian for point
            buffer.putInt(1); // Point type
            buffer.putDouble(point[0]);
            buffer.putDouble(point[1]);
        }
        return buffer;
    }

    private static ByteBuffer createMultiLineString(double[][][] lineStrings) {
        int size = 13; // 4 (SRID) + 1 (byte order) + 4 (type) + 4 (num linestrings)
        for (double[][] lineString : lineStrings) {
            size += 9 + 16 * lineString.length; // 5 (header) + 4 (num points) + 16 (2 doubles) * num points
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0); // SRID
        buffer.put((byte) 1); // Little endian
        buffer.putInt(5); // MultiLineString type
        buffer.putInt(lineStrings.length);
        for (double[][] lineString : lineStrings) {
            buffer.put((byte) 1); // Little endian for linestring
            buffer.putInt(2); // LineString type
            buffer.putInt(lineString.length);
            for (double[] point : lineString) {
                buffer.putDouble(point[0]);
                buffer.putDouble(point[1]);
            }
        }
        return buffer;
    }

    private static ByteBuffer createMultiPolygon(double[][][][] polygons) {
        int size = 13; // 4 (SRID) + 1 (byte order) + 4 (type) + 4 (num polygons)
        for (double[][][] polygon : polygons) {
            size += 9; // 5 (header) + 4 (num rings)
            for (double[][] ring : polygon) {
                size += 4 + 16 * ring.length; // 4 (num points) + 16 (2 doubles) * num points
            }
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0); // SRID
        buffer.put((byte) 1); // Little endian
        buffer.putInt(6); // MultiPolygon type
        buffer.putInt(polygons.length);
        for (double[][][] polygon : polygons) {
            buffer.put((byte) 1); // Little endian for polygon
            buffer.putInt(3); // Polygon type
            buffer.putInt(polygon.length);
            for (double[][] ring : polygon) {
                buffer.putInt(ring.length);
                for (double[] point : ring) {
                    buffer.putDouble(point[0]);
                    buffer.putDouble(point[1]);
                }
            }
        }
        return buffer;
    }

    private static ByteBuffer createGeometryCollection(List<Object[]> geometries) {
        int size = 13; // 4 (SRID) + 1 (byte order) + 4 (type) + 4 (num geometries)
        for (Object[] geom : geometries) {
            ByteBuffer tempBuffer = createGeometryBuffer((String) geom[0], geom[1]);
            size += 1 + 4 + (tempBuffer.capacity() - 9); // 1 (byte order) + 4 (type) + (data - header)
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0); // SRID
        buffer.put((byte) 1); // Little endian
        buffer.putInt(7); // GeometryCollection type
        buffer.putInt(geometries.size());
        for (Object[] geom : geometries) {
            String geomType = (String) geom[0];
            ByteBuffer tempBuffer = createGeometryBuffer(geomType, geom[1]);
            tempBuffer.position(5); // Skip SRID and byte order
            int geometryType = tempBuffer.getInt(); // Read geometry type
            buffer.put((byte) 1); // Byte order for sub-geometry (always Little Endian)
            buffer.putInt(geometryType); // Write correct geometry type
            tempBuffer.position(9); // Skip to actual geometry data
            buffer.put(tempBuffer); // Put remaining geometry data
        }
        buffer.rewind(); // Reset position to the beginning
        return buffer;
    }
}
