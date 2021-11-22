/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;

@DataPrepperPlugin(name = "no-op", pluginType = Prepper.class)
public class NoOpPrepper implements Prepper<Record<Event>, Record<Event>> {

    public NoOpPrepper() {

    }

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
