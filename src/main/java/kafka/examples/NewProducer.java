/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NewProducer extends Thread {

	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	private final Properties props = new Properties();

	public NewProducer(String topic) {
		props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093,localhost:9094");
		//		props.put("message.send.max.retries", "3");
		props.put("retries", "2");
		props.put("timeout.ms", "1");
		// Use random partitioner. Don't need the key type. Just set it to Integer.
		// The message is of type String.
		producer = new KafkaProducer<Integer, String>(props);
		this.topic = topic;
	}

	public void run() {
		int messageNo = 1;
		while (true) {
			String messageStr = new String("Message_" + messageNo);
			//			Future<RecordMetadata> send = producer.send(new ProducerRecord<Integer, String>(topic, messageStr));
//			producer.send(new ProducerRecord<Integer, String>(topic, messageStr),
//					new Callback() {
//						public void onCompletion(RecordMetadata metadata, Exception exception) {
//							System.exit(1);
//							System.out.println(exception);
//						}
//					});

			try {
				RecordMetadata recordMetadata = producer.send(new ProducerRecord<Integer, String>(topic, messageStr)).get();
				recordMetadata.offset();
			} catch (Throwable e) {
				e.printStackTrace();
			}

			//			System.out.println(messageStr);

			messageNo++;

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {

			}
		}
	}

}