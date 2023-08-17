/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector.modes;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.randomcutforest.config.ForestMode;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorMode;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessor.DEVIATION_KEY;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessor.GRADE_KEY;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.time.Instant;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@DataPrepperPlugin(name = "random_cut_forest", pluginType = AnomalyDetectorMode.class, pluginConfigurationType = RandomCutForestModeConfig.class)
public class RandomCutForestMode implements AnomalyDetectorMode {
    private static final int NUMBER_OF_TREES = 50;
    private static final double ANOMALY_RATE = 0.01;
    private static final double INITIAL_ACCEPT_FRACTION = 0.125;
    private static final double LOWER_THRESHOLD = 1.1;
    private static final double HORIZON_VALUE = 0.75;
    
    private ThresholdedRandomCutForest forest;
    private int baseDimensions;
    private int sampleSize;
    private int outputAfter;
    private int shingleSize;
    private double timeDecay;
    private List<String> keys;
    private final Lock processLock;

    private static final Logger LOG = LoggerFactory.getLogger(RandomCutForestMode.class);

    @DataPrepperPluginConstructor
    public RandomCutForestMode(final RandomCutForestModeConfig randomCutForestModeConfig) {
        this.sampleSize = randomCutForestModeConfig.getSampleSize();
        this.shingleSize = randomCutForestModeConfig.getShingleSize();
        this.outputAfter = randomCutForestModeConfig.getOutputAfter();
        this.timeDecay = randomCutForestModeConfig.getTimeDecay();
        this.processLock = new ReentrantLock();
    }
    
    @Override
    public void initialize(List<String> keys, boolean verbose) {
        this.keys = keys;
        baseDimensions = keys.size();
	    Precision precision = Precision.FLOAT_32;
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
			        .transformMethod(transformMethod)
                    .outputAfter(outputAfter)
                    .timeDecay(timeDecay / sampleSize)
			        .initialAcceptFraction(INITIAL_ACCEPT_FRACTION)
                    .autoAdjust(!verbose).build();
	    forest.setLowerThreshold(LOWER_THRESHOLD);
	    forest.setHorizon(HORIZON_VALUE);
    }
    
    @Override
    public Collection<Record<Event>> handleEvents(Collection<Record<Event>> records) {
        int timeStamp = (int)Instant.now().getEpochSecond();
        List<Record<Event>> recordsOut = new ArrayList<>();
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
                } else {
                    points[index] = value.doubleValue();
                }
                index++;
            }
            if (notFound) {
                continue;
            }
            AnomalyDescriptor result = null;

            processLock.lock();
            try {
                result = forest.process(points, timeStamp);
            } catch (final Exception e) {
                LOG.debug("Error while processing the event in RCF: ", e);
            } finally {
                processLock.unlock();
            }
            if ((result != null) && (result.getAnomalyGrade() != 0) && (result.isExpectedValuesPresent())) {
                double deviations[] = new double[keys.size()];
                if (result.getRelativeIndex() != 0 && result.isStartOfAnomaly()) {
                    for (int i = 0; i < keys.size(); i++) {
                        deviations[i] = result.getPastValues()[i];
                    }
                } else {
                    for (int i = 0; i < keys.size(); i++) {
                        deviations[i] = result.getCurrentInput()[i] - result.getExpectedValuesList()[0][i];
                    }
                }
                event.put(DEVIATION_KEY, deviations);
                event.put(GRADE_KEY, result.getAnomalyGrade());
                recordsOut.add(record);
            }
        }
        return recordsOut;
    }
}

