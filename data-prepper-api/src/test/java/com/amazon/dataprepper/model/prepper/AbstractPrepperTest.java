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

package com.amazon.dataprepper.model.prepper;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.model.record.Record;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import org.junit.Assert;
import org.junit.Test;

public class AbstractPrepperTest {

    @Test
    public void testMetrics() {
        final String prepperName = "testPrepper";
        final String pipelineName = "pipelineName";
        MetricsTestUtil.initMetrics();

        PluginSetting pluginSetting = new PluginSetting(prepperName, Collections.emptyMap());
        pluginSetting.setPipelineName(pipelineName);
        AbstractPrepper<Record<String>, Record<String>> prepper = new PrepperImpl(pluginSetting);

        prepper.execute(Arrays.asList(
                new Record<>("Value1"),
                new Record<>("Value2"),
                new Record<>("Value3")
        ));

        final List<Measurement> recordsInMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(prepperName).add(MetricNames.RECORDS_IN).toString());
        final List<Measurement> recordsOutMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(prepperName).add(MetricNames.RECORDS_OUT).toString());
        final List<Measurement> elapsedTimeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(pipelineName).add(prepperName).add(MetricNames.TIME_ELAPSED).toString());

        Assert.assertEquals(1, recordsInMeasurements.size());
        Assert.assertEquals(3.0, recordsInMeasurements.get(0).getValue(), 0);
        Assert.assertEquals(1, recordsOutMeasurements.size());
        Assert.assertEquals(6.0, recordsOutMeasurements.get(0).getValue(), 0);
        Assert.assertEquals(3, elapsedTimeMeasurements.size());
        Assert.assertEquals(1.0, MetricsTestUtil.getMeasurementFromList(elapsedTimeMeasurements, Statistic.COUNT).getValue(), 0);
        Assert.assertTrue(MetricsTestUtil.isBetween(
                MetricsTestUtil.getMeasurementFromList(elapsedTimeMeasurements, Statistic.TOTAL_TIME).getValue(),
                0.1,
                0.2));
    }

    public static class PrepperImpl extends AbstractPrepper<Record<String>, Record<String>> {
        public PrepperImpl(PluginSetting pluginSetting) {
            super(pluginSetting);
        }

        @Override
        public Collection<Record<String>> doExecute(Collection<Record<String>> records) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            return records.stream()
                    .flatMap(stringRecord -> Arrays.asList(stringRecord, stringRecord).stream())
                    .collect(Collectors.toList());
        }

        @Override
        public void shutdown() {

        }
    }
}
