package org.opensearch.dataprepper.logging;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public final class DataPrepperMarkers {
    public static final Marker EVENT = MarkerFactory.getMarker("EVENT");
    public static final Marker SENSITIVE = MarkerFactory.getMarker("SENSITIVE");

    static {
        EVENT.add(SENSITIVE);
    }

    private DataPrepperMarkers() {}
}
