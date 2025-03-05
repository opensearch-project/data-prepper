package org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handles spatial data types for OpenSearch geo_shape field type compatibility.
 * This handler supports various geometric shapes including Point, LineString, Polygon,
 * MultiPoint, MultiLineString, MultiPolygon, and GeometryCollection.
 *
 * @see <a href="https://opensearch.org/docs/latest/field-types/supported-field-types/geo-shape/">OpenSearch Geo Shape Documentation</a>

 * Supported WKT (Well-Known Text) formats:
 * <ul>
 *   <li>POINT(x y)</li>
 *   <li>LINESTRING(x y, x y, ...)</li>
 *   <li>POLYGON((x y, x y, ..., x y))</li>
 *   <li>MULTIPOINT((x y), (x y), ...)</li>
 *   <li>MULTILINESTRING((x y, x y, ...), (x y, x y, ...))</li>
 *   <li>MULTIPOLYGON(((x y, x y, ...)), ((x y, x y, ...)))</li>
 *   <li>GEOMETRYCOLLECTION(POINT(x y), LINESTRING(x y, x y))</li>
 * </ul>
 * */
public class SpatialTypeHandler implements MySQLDataTypeHandler {
    // MySQL geometry type constants
    private static final int GEOMETRY_POINT = 1;
    private static final int GEOMETRY_LINESTRING = 2;
    private static final int GEOMETRY_POLYGON = 3;
    private static final int GEOMETRY_MULTIPOINT = 4;
    private static final int GEOMETRY_MULTILINESTRING = 5;
    private static final int GEOMETRY_MULTIPOLYGON= 6;
    private static final int GEOMETRY_COLLECTION = 7;

    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        if (value == null) {
            return null;
        }

        if (value instanceof Map) {
            final Object data = ((Map<?, ?>)value).get(BYTES_KEY);

            if (data == null) {
                return null;
            }

            final String val = data.toString();
            // val.getBytes() uses the platform's default charset encoding (usually UTF-8)
            // Treats special characters as multi-byte UTF-8 characters
            // Corrupts the binary data because it's interpreting raw bytes as UTF-8 encoded text

            // Copy directly to preserve the raw byte values
            // Treats each character as a single byte
            // Maintains the original binary data structure without charset encoding
            byte[] wkbBytes = new byte[val.length()];
            for (int i = 0; i < val.length(); i++) {
                wkbBytes[i] = (byte) val.charAt(i);
            }
            return parseGeometry(wkbBytes, columnName);

        } else if (value instanceof byte[]) {
            return parseGeometry((byte[]) value, columnName);
        }

        throw new IllegalArgumentException("Unsupported value type. The value is of type: " + value.getClass());
    }

    private String parseGeometry(final byte[] rawData, final String columnName) {
        try {
            return parseGeometry(ByteBuffer.wrap(rawData).asReadOnlyBuffer());
        } catch (Exception e) {
            throw new RuntimeException("Error processing the geometry data type value for columnName: " + columnName, e);
        }
    }

    private String parseGeometry(final ByteBuffer buffer) {
        // Skip SRID (4 bytes)
        buffer.position(buffer.position() + 4);

        // Read WKB byte order (1 byte)
        final byte wkbByteOrder = buffer.get();
        final ByteOrder order = (wkbByteOrder == 1) ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        buffer.order(order);

        // Read WKB type (4 bytes)
        final int wkbType = buffer.getInt();

        switch (wkbType) {
            case GEOMETRY_POINT:
                return parsePoint(buffer);
            case GEOMETRY_LINESTRING:
                return parseLineString(buffer);
            case GEOMETRY_POLYGON:
                return parsePolygon(buffer);
            case GEOMETRY_MULTIPOINT:
                return parseMultiPoint(buffer);
            case GEOMETRY_MULTILINESTRING:
                return parseMultiLineString(buffer);
            case GEOMETRY_MULTIPOLYGON:
                return parseMultiPolygon(buffer);
            case GEOMETRY_COLLECTION:
                return parseGeometryCollection(buffer);
            default:
                throw new IllegalArgumentException("Unsupported WKB type: " + wkbType);
        }
    }

    private String parseGeometryCollection(final ByteBuffer buffer) {
        int numGeometries = buffer.getInt();

        if (numGeometries < 1) {
            throw new IllegalArgumentException("GeometryCollection must have at least 1 geometry");
        }

        List<String> geometries = new ArrayList<>();

        for (int i = 0; i < numGeometries; i++) {
            // Read WKB byte order for this geometry
            byte geomByteOrder = buffer.get();
            buffer.order(geomByteOrder == 1 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);

            // Read geometry type
            int geomType = buffer.getInt();

            // Parse the individual geometry based on its type
            String geometry;
            switch (geomType) {
                case GEOMETRY_POINT:
                    geometry = parsePoint(buffer);
                    break;
                case GEOMETRY_LINESTRING:
                    geometry = parseLineString(buffer);
                    break;
                case GEOMETRY_POLYGON:
                    geometry = parsePolygon(buffer);
                    break;
                case GEOMETRY_MULTIPOINT:
                    geometry = parseMultiPoint(buffer);
                    break;
                case GEOMETRY_MULTILINESTRING:
                    geometry = parseMultiLineString(buffer);
                    break;
                case GEOMETRY_MULTIPOLYGON:
                    geometry = parseMultiPolygon(buffer);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported geometry type in collection: " + geomType);
            }

            geometries.add(geometry);
        }

        return formatGeometryCollection(geometries);
    }

    private String parsePoint(final ByteBuffer buffer) {
        double x = buffer.getDouble();
        double y = buffer.getDouble();
        return String.format("POINT(%f %f)", x, y);
    }

    private String parseLineString(final ByteBuffer buffer) {
        int numPoints = buffer.getInt();

        if (numPoints < 2) {
            throw new IllegalArgumentException("LineString must have at least 2 points");
        }

        List<Point> points = createPoints(buffer, numPoints);

        return formatLineString(points);
    }

    private String parsePolygon(final ByteBuffer buffer) {
        int numRings = buffer.getInt();

        List<List<Point>> rings = parseLinearRing(buffer, numRings);

        return formatPolygon(rings);
    }

    private List<List<Point>> parseLinearRing(final ByteBuffer buffer, final int numRings) {
        if (numRings < 1) {
            throw new IllegalArgumentException("Polygon must have at least 1 ring");
        }

        List<List<Point>> rings = new ArrayList<>();
        for (int ring = 0; ring < numRings; ring++) {
            int numPoints = buffer.getInt();
            if (numPoints < 4) {
                throw new IllegalArgumentException("Polygon ring must have at least 4 points");
            }

            List<Point> points = createPoints(buffer, numPoints);
            rings.add(points);
        }

        return rings;
    }

    private String parseMultiPoint(final ByteBuffer buffer) {
        int numPoints = buffer.getInt();

        if (numPoints < 1) {
            throw new IllegalArgumentException("MultiPoint must have at least 1 point");
        }

        List<Point> points = new ArrayList<>();
        for (int i = 0; i < numPoints; i++) {
            // Skip WKB header for each point
            buffer.position(buffer.position() + 5);
            double x = buffer.getDouble();
            double y = buffer.getDouble();
            points.add(new Point(x, y));
        }

        return formatMultiPoint(points);
    }

    private String parseMultiLineString(final ByteBuffer buffer) {
        int numLines = buffer.getInt();

        if (numLines < 1) {
            throw new IllegalArgumentException("MultiLineString must have at least 1 line");
        }

        List<List<Point>> lines = new ArrayList<>();
        for (int i = 0; i < numLines; i++) {
            // Skip WKB header for each line
            buffer.position(buffer.position() + 5);

            int numPoints = buffer.getInt();
            if (numPoints < 2) {
                throw new IllegalArgumentException("LineString must have at least 2 points");
            }

            List<Point> points = createPoints(buffer, numPoints);
            lines.add(points);
        }

        return formatMultiLineString(lines);
    }

    private String parseMultiPolygon(final ByteBuffer buffer) {
        int numPolygons = buffer.getInt();

        if (numPolygons < 1) {
            throw new IllegalArgumentException("MultiPolygon must have at least 1 polygon");
        }

        List<List<List<Point>>> polygons = new ArrayList<>();
        for (int i = 0; i < numPolygons; i++) {
            // Skip WKB header for each polygon
            buffer.position(buffer.position() + 5);

            int numRings = buffer.getInt();
            List<List<Point>> rings = parseLinearRing(buffer, numRings);
            polygons.add(rings);
        }

        return formatMultiPolygon(polygons);
    }

    // Helper formatting methods
    private String formatGeometryCollection(final List<String> geometries) {
        StringBuilder wkt = new StringBuilder("GEOMETRYCOLLECTION(");
        for (int i = 0; i < geometries.size(); i++) {
            if (i > 0) {
                wkt.append(", ");
            }
            wkt.append(geometries.get(i));
        }
        wkt.append(")");
        return wkt.toString();
    }
    
    private String formatLineString(final List<Point> points) {
        StringBuilder wkt = new StringBuilder("LINESTRING(");
        appendPoints(wkt, points);
        wkt.append(")");
        return wkt.toString();
    }

    private void appendPoints(final StringBuilder wkt, final List<Point> points) {
        for (int i = 0; i < points.size(); i++) {
            if (i > 0) wkt.append(", ");
            Point p = points.get(i);
            wkt.append(formatPoint(p));
        }
    }

    private void appendPointsList(final StringBuilder wkt, final List<List<Point>> rings) {
        for (int j = 0; j < rings.size(); j++) {
            if (j > 0) wkt.append(", ");
            wkt.append("(");
            List<Point> ring = rings.get(j);
            appendPoints(wkt, ring);
            wkt.append(")");
        }
    }

    private static String formatPoint(final Point p) {
        return String.format("%f %f", p.x, p.y);
    }

    private String formatPolygon(final List<List<Point>> rings) {
        StringBuilder wkt = new StringBuilder("POLYGON(");
        appendPointsList(wkt, rings);
        wkt.append(")");
        return wkt.toString();
    }

    private String formatMultiPoint(final List<Point> points) {
        StringBuilder wkt = new StringBuilder("MULTIPOINT(");
        appendPoints(wkt, points);
        wkt.append(")");
        return wkt.toString();
    }

    private String formatMultiLineString(final List<List<Point>> lines) {
        StringBuilder wkt = new StringBuilder("MULTILINESTRING(");
        appendPointsList(wkt, lines);
        wkt.append(")");
        return wkt.toString();
    }

    private String formatMultiPolygon(final List<List<List<Point>>> polygons) {
        StringBuilder wkt = new StringBuilder("MULTIPOLYGON(");
        for (int i = 0; i < polygons.size(); i++) {
            if (i > 0) wkt.append(", ");
            wkt.append("(");
            List<List<Point>> rings = polygons.get(i);
            appendPointsList(wkt, rings);
            wkt.append(")");
        }
        wkt.append(")");
        return wkt.toString();
    }

    private List<Point> createPoints(final ByteBuffer buffer, final int numPoints) {
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < numPoints; i++) {
            double x = buffer.getDouble();
            double y = buffer.getDouble();
            points.add(new Point(x, y));
        }
        return points;
    }

    private static class Point {
        final double x;
        final double y;

        Point(double x, double y) {
            this.x = x;
            this.y = y;
        }
    }
}
