package org.opensearch.dataprepper.plugins.source.neptune;

import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.UsesEnhancedSourceCoordination;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.PartitionFactory;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.LeaderPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;


@DataPrepperPlugin(name = "neptune", pluginType = Source.class, pluginConfigurationType = NeptuneSourceConfig.class)
public class NeptuneSource implements Source<Record<Event>>, UsesEnhancedSourceCoordination {
    private static final Logger LOG = LoggerFactory.getLogger(NeptuneSource.class);

    private final PluginMetrics pluginMetrics;
    private final NeptuneSourceConfig sourceConfig;
    private final PluginConfigObservable pluginConfigObservable;
    private EnhancedSourceCoordinator sourceCoordinator;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private NeptuneService neptuneService;

    private final boolean acknowledgementsEnabled;

    @DataPrepperPluginConstructor
    public NeptuneSource(final PluginMetrics pluginMetrics,
                         final NeptuneSourceConfig sourceConfig,
                         final AcknowledgementSetManager acknowledgementSetManager,
                         final PluginConfigObservable pluginConfigObservable) {
        this.pluginMetrics = pluginMetrics;
        this.sourceConfig = sourceConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pluginConfigObservable = pluginConfigObservable;
        this.acknowledgementsEnabled = sourceConfig.isAcknowledgments();
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        Objects.requireNonNull(sourceCoordinator);
        sourceCoordinator.createPartition(new LeaderPartition());

        try {
            neptuneService = new NeptuneService(sourceCoordinator, sourceConfig, pluginMetrics, acknowledgementSetManager);
            LOG.info("Start Neptune service");
            neptuneService.start(buffer);
        } catch (NeptuneSigV4SignerException e) {
            LOG.error("SigV4 ERROR");
        }
    }


    @Override
    public void stop() {
        LOG.info("Stop Neptune service");
        if (Objects.nonNull(neptuneService)) {
            neptuneService.shutdown();
        }
    }

    @Override
    public void setEnhancedSourceCoordinator(EnhancedSourceCoordinator sourceCoordinator) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceCoordinator.initialize();
    }

    @Override
    public Function<SourcePartitionStoreItem, EnhancedSourcePartition> getPartitionFactory() {
        return new PartitionFactory();
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }
}
