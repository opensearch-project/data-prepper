package org.opensearch.dataprepper.plugins.source.crowdstrike;


import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.crowdstrike.rest.CrowdStrikeAuthClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.CrawlerApplicationContextMarker;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourcePlugin;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.TimeSliceCrawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeLeaderProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.PLUGIN_NAME;


/**
 * CrowdStrike connector entry point.
 * 🚧 Work in progress — under active development.
 * Not ready for production use.
 */
@Experimental
@DataPrepperPlugin(name = PLUGIN_NAME,
        pluginType = Source.class,
        pluginConfigurationType = CrowdStrikeSourceConfig.class,
        packagesToScan = {CrawlerApplicationContextMarker.class, CrowdStrikeSource.class}
)
public class CrowdStrikeSource extends CrawlerSourcePlugin {

    private static final Logger log = LoggerFactory.getLogger(CrowdStrikeSource.class);
    private final CrowdStrikeSourceConfig sourceConfig;
    private final CrowdStrikeAuthClient authClient;

    @DataPrepperPluginConstructor
    public CrowdStrikeSource(final CrowdStrikeSourceConfig sourceConfig,
                             final PluginMetrics pluginMetrics,
                             final PluginFactory pluginFactory,
                             final AcknowledgementSetManager acknowledgementSetManager,
                             final CrowdStrikeAuthClient authClient,
                             TimeSliceCrawler crawler, PluginExecutorServiceProvider executorServiceProvider) {
        super(PLUGIN_NAME, pluginMetrics, sourceConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider);
        log.info("Creating CrowdStrike Source Plugin");
        this.sourceConfig = sourceConfig;
        this.authClient = authClient;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        log.info("Starting CrowdStrike Source Plugin...");
        authClient.initCredentials();
        super.start(buffer);
    }

    @Override
    protected LeaderProgressState createLeaderProgressState() {
        return new CrowdStrikeLeaderProgressState(Instant.now(), sourceConfig.getLookBackDays());
    }


    @Override
    public void stop() {
        log.info("Stopping CrowdStrike Source Plugin...");
        super.stop();
    }
}
