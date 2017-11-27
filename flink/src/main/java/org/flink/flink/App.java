package org.flink.flink;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

/**
 * Hello world!
 *
 */
public class App {

	private static final String KAFKA_BROKER = "localhost:9092";
	private static final String KAFKA_INPUT_TOPIC = "test";
	private static final String KAFKA_GROUP_ID = "g1";
	private static final String CLASS_NAME = App.class.getSimpleName();

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);

		// kafka source
		// https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/connectors/kafka.html#kafka-consumer
		Properties prop = new Properties();
		prop.put("bootstrap.servers", KAFKA_BROKER);
		prop.put("group.id", KAFKA_GROUP_ID);
		prop.put("auto.offset.reset", "latest");
		prop.put("enable.auto.commit", "false");

		FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<>(
				KAFKA_INPUT_TOPIC, new SimpleStringSchema(), prop);

		// checkpoints
		// internals:
		// https://ci.apache.org/projects/flink/flink-docs-master/internals/stream_checkpointing.html#checkpointing
		// config:
		// https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/stream/checkpointing.html

		env.addSource(source).keyBy((any) -> 1).flatMap(new StatefulMapper()).print();

		env.execute(CLASS_NAME);
	}

	/* *****************************************************************
	 * Stateful mapper (cf.
	 * https://ci.apache.org/projects/flink/flink-docs-release
	 * -1.3/dev/stream/state.html)
	 * ***************************************************************
	 */

	public static class StatefulMapper extends
			RichFlatMapFunction<String, String> {
		private static final long serialVersionUID = 1L;
		private transient ValueState<Integer> state;

		@Override
		public void flatMap(String record, Collector<String> collector)
				throws Exception {
			// access the state value
			Integer currentState = state.value();

			// update the counts
			currentState += 1;
			collector.collect(String.format("%s: (%s,%d)", LocalDateTime.now()
					.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), record,
					currentState));
			// update the state
			state.update(currentState);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
					"App", TypeInformation.of(Integer.class));
			state = getRuntimeContext().getState(descriptor);
		}
	}
}
