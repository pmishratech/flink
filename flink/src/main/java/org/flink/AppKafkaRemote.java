package org.flink;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Hello world!
 *
 */
public class AppKafkaRemote {

	public static void main(String[] args) {
		final String jobManagerAddress = "localhost";
		final int jobManagerPort = ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(jobManagerAddress, jobManagerPort);
		
		env.enableCheckpointing(20);
		
		try {
			env.setStateBackend(new FsStateBackend("checkpoints"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test1");

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>(
				"test", new SimpleStringSchema(), properties);
		env.addSource(myConsumer).writeAsText("file1");
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
