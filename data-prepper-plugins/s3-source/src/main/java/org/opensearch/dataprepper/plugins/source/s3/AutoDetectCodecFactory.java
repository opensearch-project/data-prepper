/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.formatdetection.DetectedFormat;
import org.opensearch.dataprepper.plugins.formatdetection.FormatDetectionResult;
import org.opensearch.dataprepper.plugins.formatdetection.FormatDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Selects the appropriate InputCodec based on format detection results.
 * Used when no codec is explicitly configured in the S3 source (auto-detect mode).
 */
class AutoDetectCodecFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AutoDetectCodecFactory.class);

    private final PluginFactory pluginFactory;
    private final FormatDetector formatDetector;
    private final Map<DetectedFormat, InputCodec> codecCache;

    AutoDetectCodecFactory(final PluginFactory pluginFactory) {
        this.pluginFactory = pluginFactory;
        this.formatDetector = new FormatDetector();
        this.codecCache = new ConcurrentHashMap<>();
    }

    FormatDetector getFormatDetector() {
        return formatDetector;
    }

    InputCodec getCodecForFormat(final DetectedFormat format) {
        return codecCache.computeIfAbsent(format, this::createCodec);
    }

    private InputCodec createCodec(final DetectedFormat format) {
        final String pluginName = mapFormatToCodecPlugin(format);
        if (pluginName == null) {
            LOG.warn("No codec available for detected format: {}", format);
            return null;
        }

        LOG.info("Auto-detecting codec: {} → {}", format, pluginName);
        final PluginSetting pluginSetting = new PluginSetting(pluginName, Collections.emptyMap());
        return pluginFactory.loadPlugin(InputCodec.class, pluginSetting);
    }

    private String mapFormatToCodecPlugin(final DetectedFormat format) {
        if (format == DetectedFormat.NDJSON) return "ndjson";
        if (format == DetectedFormat.JSON) return "json";
        if (format == DetectedFormat.CSV) return "csv";
        if (format == DetectedFormat.TSV) return "csv";
        if (format == DetectedFormat.PARQUET) return "parquet";
        if (format == DetectedFormat.AVRO) return "avro";
        if (format == DetectedFormat.TEXT) return "newline";
        return null;
    }
}
