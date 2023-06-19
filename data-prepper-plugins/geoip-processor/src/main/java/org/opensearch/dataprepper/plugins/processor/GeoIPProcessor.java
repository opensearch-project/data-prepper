/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import java.net.MalformedURLException;
import java.util.Collection;

/**
 * Implementation class of geoIP-processor plugin. It is responsible for enrichment of
 * attributes for the public IPs. Supports both IPV4 and IPV6
 */
@DataPrepperPlugin(name = "geoip", pluginType = Processor.class, pluginConfigurationType = GeoIPProcessorConfig.class)
public class GeoIPProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

  /**
   * GeoIPProcessor constructor for initialization of required attributes
   * @param pluginSetting pluginSetting
   * @param geoCodingProcessorConfig geoCodingProcessorConfig
   * @param pluginFactory pluginFactory
   * @throws MalformedURLException MalformedURLException
   */
  @DataPrepperPluginConstructor
  public GeoIPProcessor(PluginSetting pluginSetting,
                        final GeoIPProcessorConfig geoCodingProcessorConfig,
                        final PluginFactory pluginFactory) throws MalformedURLException {
    super(pluginSetting);
    //TODO
  }

  /**
   * Get the enriched data from the maxmind database
   * @param records Input records
   * @return collection of record events
   */
  @Override
  public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {

    //TODO : logic call the enrichment of data class methods
    return null;
  }

  @Override
  public void prepareForShutdown() {
    //TODO
  }

  @Override
  public boolean isReadyForShutdown() {
    //TODO
    return false;
  }

  @Override
  public void shutdown() {
    //TODO
  }
}