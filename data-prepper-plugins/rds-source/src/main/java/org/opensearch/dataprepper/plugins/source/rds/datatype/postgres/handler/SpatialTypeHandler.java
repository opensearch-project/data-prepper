package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.utils.PgArrayParser;
import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGlseg;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SpatialTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isSpatial()) {
            throw new IllegalArgumentException("ColumnType is not spatial: " + columnType);
        }
        if(columnType.isSubCategoryArray())
            return PgArrayParser.parseTypedArray(value.toString(), PostgresDataType.getScalarType(columnType),
                    this::parseGeometry);
        return parseGeometry(columnType, value.toString());
    }

    private Object parseGeometry(final PostgresDataType columnType, final String val) {
        switch (columnType) {
            case POINT:
                return parsePoint(val);
            case LINE:
                return parseLine(val);
            case LSEG:
                return parseLseg(val);
            case BOX:
                return parseBox(val);
            case PATH:
                return parsePath(val);
            case POLYGON:
                return parsePolygon(val);
            case CIRCLE:
                return parseCircle(val);
            default:
                return val;
        }
    }

    private String parsePoint(final String val) {
        try {
            PGpoint point = new PGpoint(val);
            return String.format("POINT(%f %f)", point.x, point.y);
        } catch (SQLException e) {
            throw new RuntimeException("Error converting String to PGpoint object", e);
        }
    }

    private String parseLine(final String val) {
        try {
            PGline line = new PGline(val);
            PGpoint[] points = getPointsOnLine(line);
            return formatLine(points);
        } catch (SQLException e) {
            throw new RuntimeException("Error converting String to PGpoint object", e);
        }
    }

    private String parseLseg(final String val) {
        try {
            PGlseg lseg = new PGlseg(val);
            PGpoint[] points = lseg.point;
            if (points == null || points.length != 2)
                throw new IllegalArgumentException("LineSegment must have at least 2 points");
            return formatLine(points);
        } catch (SQLException e) {
            throw new RuntimeException("Error converting String to PGlseg object", e);
        }
    }

    private String parseBox(final String val) {
        try {
            PGbox box = new PGbox(val);
            PGpoint[] points = box.point;
            if (points == null || points.length != 2) {
                throw new IllegalArgumentException("Box must have exactly 2 points");
            }
            return formatBox(points);
        } catch (SQLException e) {
            throw new RuntimeException("Error converting String to PGbox object", e);
        }
    }

    private String parsePath(final String val) {
        try {
            PGpath path = new PGpath(val);
            PGpoint[] points = path.points;
            if (points == null || points.length == 0)
                throw new IllegalArgumentException("Path must have at least 1 point");
            return formatPath(points, path.open);
        } catch (SQLException e) {
            throw new RuntimeException("Error converting String to PGpath object", e);
        }
    }

    private Object parseCircle(String val) {
        try {
            PGcircle circle = new PGcircle(val);
            if (circle.center == null ) {
                throw new IllegalArgumentException("Circle must have a center point");
            }
            return formatCircle(circle);
        } catch (SQLException e) {
            throw new RuntimeException("Error converting String to PGcircle object", e);
        }
    }

    private String parsePolygon(String val) {
        try {
            PGpolygon polygon = new PGpolygon(val);
            PGpoint[] points = polygon.points;
            if (points == null || points.length == 0)
                throw new IllegalArgumentException("Path must have at least 1 point");
            return formatPolygon(points);
        } catch (SQLException e) {
            throw new RuntimeException("Error converting String to PGpolygon object", e);
        }
    }

    private PGpoint[] getPointsOnLine(PGline line) {
        double a = line.a;
        double b = line.b;
        double c = line.c;
        PGpoint p1, p2;
        if (b != 0) {
            // Case 1: b is not zero, we can solve for y
            double x1 = 0.0;
            double y1 = -c / b;
            double x2 = 1.0;
            double y2 = -(c + a) / b;
            p1 = new PGpoint(x1, y1);
            p2 = new PGpoint(x2, y2);
        } else if (a != 0) {
            // Case 2: b is zero, a is not zero, we solve for x
            double y1 = 0.0;
            double x1 = -c / a;
            double y2 = 1.0;
            double x2 = -(c + b) / a;
            p1 = new PGpoint(x1, y1);
            p2 = new PGpoint(x2, y2);
        } else {
            // Case 3: Both a and b are zero, which should never happen for a valid line
            throw new IllegalArgumentException("Invalid line: both a and b cannot be zero");
        }

        return new PGpoint[]{p1, p2};
    }

    private static String formatPoint(final PGpoint p) {
        return String.format("%f %f", p.x, p.y);
    }

    private void appendPoints(final StringBuilder wkt, final PGpoint[] points) {
        for (int i = 0; i < points.length; i++) {
            if (i > 0) wkt.append(", ");
            wkt.append(formatPoint(points[i]));
        }
    }

    private String formatLine(final PGpoint[] points) {
        StringBuilder wkt = new StringBuilder("LINESTRING(");
        appendPoints(wkt, points);
        wkt.append(")");
        return wkt.toString();
    }

    private String formatBox(final PGpoint[] points) {
        StringBuilder wkt = new StringBuilder("POLYGON((");
        appendPoints(wkt, new PGpoint[]{
                points[0],
                new PGpoint(points[1].x, points[0].y),
                points[1],
                new PGpoint(points[0].x, points[1].y),
                points[0]  // Close the polygon
        });
        wkt.append("))");
        return wkt.toString();
    }

    private String formatPath(final PGpoint[] points, final boolean isOpenPath) {
        StringBuilder wkt = new StringBuilder();
        wkt.append(isOpenPath ? "LINESTRING(" : "POLYGON((");
        appendPoints(wkt, points);
        //For closed paths first and last point must be same
        if (!isOpenPath && !points[0].equals(points[points.length - 1])) {
            wkt.append(", ").append(formatPoint(points[0]));
        }
        wkt.append(isOpenPath ? ")" : "))");
        return wkt.toString();
    }

    private String formatPolygon(final PGpoint[] points) {
        StringBuilder wkt = new StringBuilder("POLYGON((");
        appendPoints(wkt, points);
        if (!points[0].equals(points[points.length - 1])) {
            wkt.append(", ").append(formatPoint(points[0]));
        }
        wkt.append("))");
        return wkt.toString();
    }
    /**
     * Converts a PostgreSQL circle (PGcircle) object to a Map representation.
     *
     * This method takes a PGcircle object and creates a Map with two keys:
     * 1. "center": A nested Map containing the x and y coordinates of the circle's center.
     * 2. "radius": The radius of the circle.
     *
     * @param circle The PGcircle object to be converted.
     * @return A Map containing the circle's center coordinates and radius.
     *         The structure of the returned Map is:
     *         {
     *           "center": {
     *             "x": (x-coordinate),
     *             "y": (y-coordinate)
     *           },
     *           "radius": (radius value)
     *         }
     */
    private Object formatCircle(final PGcircle circle) {
        PGpoint center = circle.center;
        Map<String, Object> circleObject = new HashMap<>();
        circleObject.put("center", Map.of("x", center.x, "y", center.y));
        circleObject.put("radius", circle.radius);
        return circleObject;
    }
}
