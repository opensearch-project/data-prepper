package com.amazon.situp.plugins.source;

import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * A simple source which reads data from console each line at a time. It exits when it reads case insensitive "exit"
 * from console or if Pipeline notifies to stop.
 */
@SitupPlugin(name = "stdin", type = PluginType.SOURCE)
public class StdInSource implements Source<Record<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(StdInSource.class);
    private static final String ATTRIBUTE_TIMEOUT = "write_timeout";
    private static final int WRITE_TIMEOUT = 5_000;
    private final Scanner reader;
    private final int writeTimeout;
    private final String pipelineName;
    private boolean isStopRequested;

    /**
     * Mandatory constructor for SITUP Component - This constructor is used by SITUP
     * runtime engine to construct an instance of {@link StdInSource} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public StdInSource(final PluginSetting pluginSetting) {
        this(pluginSetting.getIntegerOrDefault(ATTRIBUTE_TIMEOUT, WRITE_TIMEOUT), pluginSetting.getPipelineName());
    }

    public StdInSource(final int writeTimeout, final String pipelineName) {
        this.writeTimeout = writeTimeout;
        this.pipelineName = pipelineName;
        this.reader = new Scanner(System.in);
        isStopRequested = false;
    }

    @Override
    public void start(final Buffer<Record<String>> buffer) {
        checkNotNull(buffer, format("Pipeline [%s] - buffer cannot be null for source to start", pipelineName));
        String line = reader.nextLine();
        while (!"exit".equalsIgnoreCase(line) && !isStopRequested) {
            final Record<String> record = new Record<>(line);
            try{
                buffer.write(record, writeTimeout);
            } catch (TimeoutException ex) {
                LOG.error("Pipeline [{}] - Timed out writing to buffer; Will exit without further processing",
                        pipelineName, ex);
                throw new RuntimeException(format("Pipeline [%s] - Timed out writing to buffer", pipelineName), ex);
            }
            line = reader.nextLine();
        }
    }

    @Override
    public void stop() {
        isStopRequested = true;
    }
}