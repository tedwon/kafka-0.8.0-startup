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


import com.sun.javafx.collections.transformation.SortedList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import kafka.api.OffsetRequest;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import scala.Option;
import scala.collection.*;
import scala.collection.Iterable;


public class Consumer extends Thread {

  private final ConsumerConnector consumer;
  private final String topic;

  public Consumer(String topic) {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
        createConsumerConfig());
    this.topic = topic;
  }

  private static ConsumerConfig createConsumerConfig() {
    Properties props = new Properties();
    props.put("zookeeper.connect", KafkaProperties.zkConnect);
    props.put("group.id", KafkaProperties.groupId);
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", OffsetRequest.SmallestTimeString());

    return new ConsumerConfig(props);

  }

  public void run() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(4));
    Map<String, List<KafkaStream<byte[], byte[]>>>
        consumerMap =
        consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

    System.out.println("size=" + streams.size());

    Map<Integer, KafkaStream<byte[], byte[]>> map = new TreeMap();

    for (KafkaStream<byte[], byte[]> stream : streams) {
      MessageAndMetadata<byte[], byte[]> head = (MessageAndMetadata<byte[], byte[]>)stream.head();
      int partition = head.partition();
      System.out.println(partition);
      map.put(partition, stream);
    }

    System.out.println(map);

//    KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
//    ConsumerIterator<byte[], byte[]> it = stream.iterator();
//
//    while (it.hasNext()) {
//      MessageAndMetadata<byte[], byte[]> next = it.next();
//      int partition = next.partition();
//      System.out.println(partition);
////      System.out.println(System.currentTimeMillis() + " : " + new String(it.next().message()));
//    }


//    KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(1);
//    String s = stream.clientId();
//    System.out.println("###=" + s);
  }
}
