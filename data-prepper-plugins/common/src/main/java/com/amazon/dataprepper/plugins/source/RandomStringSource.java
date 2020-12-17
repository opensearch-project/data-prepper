package com.amazon.dataprepper.plugins.source;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@DataPrepperPlugin(name = "random", type = PluginType.SOURCE)
public class RandomStringSource implements Source<Record<String>> {

    private ExecutorService executorService;
    private boolean stop = false;

    public RandomStringSource(final PluginSetting pluginSetting) {

    }

    private void setExecutorService() {
        if(executorService == null || executorService.isShutdown()) {
            executorService = Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().setDaemon(false).setNameFormat("random-source-pool-%d").build()
            );
        }
    }

    @Override
    public void start(Buffer<Record<String>> buffer) {
        setExecutorService();
        executorService.execute(() -> {
            while (!stop) {
                try {
                    buffer.write(new Record<>(UUID.randomUUID().toString()), 500);
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                } catch (TimeoutException e) {
                    // Do nothing
                }
            }
        });
    }

    @Override
    public void stop() {
        stop = true;
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            executorService.shutdownNow();
        }
    }
}
