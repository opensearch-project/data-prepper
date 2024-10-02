/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.test;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DataPrepperPlugin(name = "test_di_source",
        alternateNames = { "test_source_alternate_name1", "test_source_alternate_name2" },
        deprecatedName = "test_source_deprecated_name",
        pluginType = Source.class,
        packagesToScan = {TestDISource.class})
public class TestDISource extends TestSource {
    public static final List<Record<String>> TEST_DATA = Stream.of("TEST")
            .map(Record::new).collect(Collectors.toList());

    private final TestComponent testComponent;


    public TestDISource(TestComponent testComponent) {
        this.testComponent = testComponent;
    }

    @Override
    public void start(Buffer<Record<String>> buffer) {
        final Iterator<Record<String>> iterator = TEST_DATA.iterator();
            while (iterator.hasNext()) {
                try {
                    buffer.write(iterator.next(), 1_000);
                } catch (TimeoutException e) {
                    throw new RuntimeException("Timed out writing to buffer");
                }
            }
    }

    @Override
    public void stop() {}
}
