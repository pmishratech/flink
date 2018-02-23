package org.flink;

import java.util.Properties;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class AppKafkaProducer {

	public static void main(String[] args) throws Exception {
		// Properties for Kafka
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("topic", "test");
		kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
		kafkaProps.setProperty("group.id", "flink-test");

		// Flink environment setup
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		// Flink check/save point setting
		env.enableCheckpointing(20);
		env.getCheckpointConfig().setCheckpointingMode(
				CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig()
				.enableExternalizedCheckpoints(
						CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<String>(
				"test", new SimpleStringSchema(), kafkaProps);
		
		DataStream<String> stream = env.addSource(consumer);

		FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(
				"localhost:9092", // broker list
				"my-topic", // target topic
				new SimpleStringSchema()); // serialization schema

		// the following is necessary for at-least-once delivery guarantee
		myProducer.setLogFailuresOnly(false); // "false" by default
		myProducer.setFlushOnCheckpoint(true); // "false" by default

		stream.addSink(myProducer);

		env.execute();
	}
}