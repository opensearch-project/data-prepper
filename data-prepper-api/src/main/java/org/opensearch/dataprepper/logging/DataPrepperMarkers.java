/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.logging;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public final class DataPrepperMarkers {
    public static final Marker EVENT = MarkerFactory.getMarker("EVENT");
    public static final Marker SENSITIVE = MarkerFactory.getMarker("SENSITIVE");
    public static final Marker NOISY = MarkerFactory.getMarker("NOISY");

    static {
        EVENT.add(SENSITIVE);
    }

    private DataPrepperMarkers() {}
}
