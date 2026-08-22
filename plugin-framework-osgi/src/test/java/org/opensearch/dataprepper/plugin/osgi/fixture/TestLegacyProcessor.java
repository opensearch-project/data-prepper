/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin.osgi.fixture;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * A minimal test fixture processor annotated with {@link DataPrepperPlugin}.
 * Used by {@link org.opensearch.dataprepper.plugin.osgi.BackwardCompatibilityTest}
 * to verify that a legacy JAR without OSGi manifest is rejected at load time
 * with a fail-fast error.
 */
@DataPrepperPlugin(name = "test_legacy_processor", pluginType = Processor.class)
public class TestLegacyProcessor implements Processor<Record<Event>, Record<Event>> {

    @Override
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        return records;
    }

    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }
}
