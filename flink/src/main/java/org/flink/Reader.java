package org.flink;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Hello world!
 *
 */
public class Reader {

	public static void main(String[] args) throws IOException {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>(
				"test", new SimpleStringSchema(), properties);
		
		env.addSource(myConsumer).map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				System.out.println(value);
				return value;
			}
		});
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
