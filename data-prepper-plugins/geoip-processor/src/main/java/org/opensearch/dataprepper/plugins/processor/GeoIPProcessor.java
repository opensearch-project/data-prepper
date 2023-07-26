/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.configuration.KeysConfig;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.EnrichFailedException;
import org.opensearch.dataprepper.plugins.processor.utils.IPValidationcheck;
import org.opensearch.dataprepper.logging.DataPrepperMarkers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Implementation class of geoIP-processor plugin. It is responsible for enrichment of
 * attributes for the public IPs. Supports both IPV4 and IPV6
 */
@DataPrepperPlugin(name = "geoip", pluginType = Processor.class, pluginConfigurationType = GeoIPProcessorConfig.class)
public class GeoIPProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

  private static final Logger LOG = LoggerFactory.getLogger(GeoIPProcessor.class);
  private static final String GEO_IP_PROCESSING_MATCH = "geoIpProcessingMatch";
  private static final String GEO_IP_PROCESSING_MISMATCH = "geoIpProcessingMismatch";
  private final Counter geoIpProcessingMatchCounter;
  private final Counter geoIpProcessingMismatchCounter;
  private final GeoIPProcessorConfig geoIPProcessorConfig;
  private final String tempPath;
  private final List<String> tagsOnSourceNotFoundFailure;
  private GeoIPProcessorService geoIPProcessorService;
  private static final String TEMP_PATH_FOLDER = "GeoIP";

  /**
   * GeoIPProcessor constructor for initialization of required attributes
   * @param pluginSetting pluginSetting
   * @param geoCodingProcessorConfig geoCodingProcessorConfig
   */
  @DataPrepperPluginConstructor
  public GeoIPProcessor(PluginSetting pluginSetting,
                        final GeoIPProcessorConfig geoCodingProcessorConfig) {
    super(pluginSetting);
    this.geoIPProcessorConfig = geoCodingProcessorConfig;
    this.tempPath = System.getProperty("java.io.tmpdir")+ File.separator + TEMP_PATH_FOLDER;
    geoIPProcessorService = new GeoIPProcessorService(geoCodingProcessorConfig,tempPath);
    tagsOnSourceNotFoundFailure = geoCodingProcessorConfig.getTagsOnSourceNotFoundFailure();
    this.geoIpProcessingMatchCounter = pluginMetrics.counter(GEO_IP_PROCESSING_MATCH);
    this.geoIpProcessingMismatchCounter = pluginMetrics.counter(GEO_IP_PROCESSING_MISMATCH);
  }

  /**
   * Get the enriched data from the maxmind database
   * @param records Input records
   * @return collection of record events
   */
  @Override
  public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {

    Map<String, Object> geoData;
    String ipAddress = null;
    Integer target;

    for (final Record<Event> eventRecord : records) {
      Event event = eventRecord.getData();
      for (KeysConfig key : geoIPProcessorConfig.getKeysConfig()) {
        List<String> sources = key.getKeyConfig().getSource();
        List<String> attributes = key.getKeyConfig().getAttributes();
        List<String> targets = key.getKeyConfig().getTarget();
        target = 0;
        if(targets.size() == sources.size()) {
          for (String source : sources) {
            ipAddress = event.get(source, String.class);
            //Lookup from DB
            if (ipAddress != null && (!(ipAddress.isEmpty()))) {
              try {
                if (IPValidationcheck.isPublicIpAddress(ipAddress)) {
                  geoData = geoIPProcessorService.getGeoData(InetAddress.getByName(ipAddress), attributes);
                  eventRecord.getData().put(targets.get(target++), geoData);
                  geoIpProcessingMatchCounter.increment();
                }
              } catch (IOException | EnrichFailedException ex) {
                geoIpProcessingMismatchCounter.increment();
                event.getMetadata().addTags(tagsOnSourceNotFoundFailure);
                LOG.error(DataPrepperMarkers.EVENT, "Failed to get Geo data for event: [{}] for the IP address [{}]", event, ipAddress, ex);
              }
            } else {
              //No Enrichment.
              event.getMetadata().addTags(tagsOnSourceNotFoundFailure);
            }
          }
        }
      }
    }
    return records;
  }

  @Override
  public void prepareForShutdown() {
    LOG.info("GeoIP plugin prepare For Shutdown");
  }

  @Override
  public boolean isReadyForShutdown() {
    return false;
  }

  @Override
  public void shutdown() {
    LOG.info("GeoIP plugin Shutdown");
  }
}