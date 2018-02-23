package org.flink;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Hello world!
 *
 */
public class ExternalisedAppStateBackend {

	public static void main(String[] args) throws IOException {
		
		Configuration config = new Configuration();
		config.setString("state.backend", "filesystem");
		config.setString("state.checkpoints.dir", "file:///flink-1.3.2/log/checkpoint/");
		config.setString("state.backend.fs.checkpointdir", "file:///flink-1.3.2/log/checkpoint-data/");
		StreamExecutionEnvironment env = new LocalStreamEnvironment(config);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");
		
		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>(
				"test", new SimpleStringSchema(), properties);
		
		DataStream<String> stream = env.addSource(myConsumer);
		
		
		FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(
		        "localhost:9092",            // broker list
		        "my-topic",                  // target topic
		        new SimpleStringSchema());   // serialization schema

		stream.addSink(myProducer);
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
