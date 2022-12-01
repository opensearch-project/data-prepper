/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;

import com.amazon.randomcutforest.config.ForestMode;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.time.Instant;

@DataPrepperPlugin(name = "anomaly_detector", pluginType = Processor.class, pluginConfigurationType = AnomalyDetectorProcessorConfig.class)
public class AnomalyDetectorProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    public static final String DEVIATION_KEY = "deviation_from_expected";
    public static final String GRADE_KEY = "grade";
    public static final String CONFIDENCE_KEY = "confidence";
    private static final int NUMBER_OF_TREES = 50;
    private static final int TRANSFORM_OUTPUT_AFTER = 32;
    private static final double ANOMALY_RATE = 0.01;
    private static final double INITIAL_ACCEPT_FRACTION = 0.125;
    private static final double LOWER_THRESHOLD = 1.1;
    private static final double HORIZON_VALUE = 0.75;

    private ThresholdedRandomCutForest forest;
    private List<String> keys;
    private int baseDimensions;

    @DataPrepperPluginConstructor
    public AnomalyDetectorProcessor(PluginMetrics pluginMetrics, final AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig) {
        super(pluginMetrics);
        String mode = anomalyDetectorProcessorConfig.getMode();
        keys = anomalyDetectorProcessorConfig.getKeys();
        baseDimensions = keys.size();
        int sampleSize = anomalyDetectorProcessorConfig.getSampleSize();
        int shingleSize = anomalyDetectorProcessorConfig.getShingleSize();
        double timeDecay = anomalyDetectorProcessorConfig.getTimeDecay();
	    Precision precision = Precision.FLOAT_32;
	    int dataSize = 4 * sampleSize;
	    int dimensions = baseDimensions * shingleSize;
	    TransformMethod transformMethod = TransformMethod.NORMALIZE;

	    forest = ThresholdedRandomCutForest.builder()
                    .compact(true)
                    .dimensions(dimensions)
			        .randomSeed(0)
                    .numberOfTrees(NUMBER_OF_TREES)
                    .shingleSize(shingleSize)
                    .sampleSize(sampleSize)
			        .internalShinglingEnabled(true)
                    .precision(precision)
                    .anomalyRate(ANOMALY_RATE)
                    .forestMode(ForestMode.STANDARD)
			        .transformMethod(transformMethod).outputAfter(TRANSFORM_OUTPUT_AFTER)
                    .timeDecay(timeDecay / sampleSize)
			        .initialAcceptFraction(INITIAL_ACCEPT_FRACTION).build();
	    forest.setLowerThreshold(LOWER_THRESHOLD);
	    forest.setHorizon(HORIZON_VALUE);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        List<Record<Event>> recordsOut = new ArrayList<>();
        int timeStamp = (int)Instant.now().getEpochSecond();
        for(final Record<Event> record : records) {
	        Event event = record.getData();
            boolean notFound = false;
            double[] points = new double[keys.size()];
            int index = 0;
            for (final String key: keys) {
                Number value = event.get(key, Number.class);
                if (value == null) {
                    notFound = true;
                    break;
                }
                if (value instanceof Long) {
                    points[index] = (double)value.longValue();
                } else if (value instanceof Integer) {
                    points[index] = (double)value.intValue();
                } else if (value instanceof Short) {
                    points[index] = (double)value.shortValue();
                } else if (value instanceof Byte) {
                    points[index] = (double)value.byteValue();
                } else if (value instanceof Float) {
                    points[index] = (double)value.floatValue();
                } else { // double
                    points[index] = (double)value.doubleValue();
                }
                index++;
            }
            if (notFound) {
                continue;
            }
	        try {
                AnomalyDescriptor result = forest.process(points, timeStamp);
                if ((result.getAnomalyGrade() != 0) && (result.isExpectedValuesPresent())) {
                    if (result.getRelativeIndex() != 0 && result.isStartOfAnomaly()) {
                        event.put(DEVIATION_KEY, result.getPastValues()[0]);
                    } else {
                        event.put(DEVIATION_KEY, result.getCurrentInput()[0] - result.getExpectedValuesList()[0][0]);
                    }
                    event.put(GRADE_KEY, result.getAnomalyGrade());
                    event.put(CONFIDENCE_KEY, result.getDataConfidence());
                    recordsOut.add(record);
                }
            } catch (Exception e) {}
        }
        return recordsOut;
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
