/**
 * 
 */
package org.opensearch.dataprepper.plugins.sink;

import java.util.Collection;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DataPrepperPlugin(name = "s3", pluginType = Sink.class, pluginConfigurationType = S3SinkConfig.class)
public class S3Sink implements Sink<Record<Object>> {
	
	private static final Logger LOG = LoggerFactory.getLogger(S3Sink.class);
	private final S3SinkConfig s3SinkConfig;
	
	private final String outputS3Path;
	private static final String SAMPLE_S3_PATH = "src/resources/";
	public static final String PATH = "path";
	
	
	@DataPrepperPluginConstructor
	public S3Sink(final S3SinkConfig s3SinkConfig, final PluginSetting pluginSetting) {
		this.s3SinkConfig = s3SinkConfig;
		final String outputS3 = (String) pluginSetting.getAttributeFromSettings(PATH);
		outputS3Path = outputS3 == null ? SAMPLE_S3_PATH : outputS3;
		
	}

	@Override
	public void output(Collection<Record<Object>> records) {
		
		final S3SinkService s3SinkService = new S3SinkService(s3SinkConfig);
		
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

}
