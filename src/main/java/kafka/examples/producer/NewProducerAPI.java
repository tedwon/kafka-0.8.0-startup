package kafka.examples.producer;

import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by ted.won on 4/23/15.
 */
public class NewProducerAPI {

	private final Properties props = new Properties();

	public void test() {
		ProducerRecord record = new ProducerRecord<String, String>("topic", "value");

		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//    props.put("metadata.broker.list", "localhost:9091");
		props.put("metadata.broker.list", "localhost:9091,localhost:9092,localhost:9093,localhost:9094");

		KafkaProducer producer = new KafkaProducer(props);

		producer.send(record);

		final Callback callback = new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception exception) {

			}
		};

		producer.send(record, callback);

	}
}
