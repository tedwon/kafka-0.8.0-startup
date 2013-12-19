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
package kafka.examples.consumer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.examples.KafkaProperties;
import kafka.examples.Producer;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

public class SimpleConsumerDemo {

  private static void printMessages(ByteBufferMessageSet messageSet)
      throws UnsupportedEncodingException {
    for (MessageAndOffset messageAndOffset : messageSet) {
      ByteBuffer payload = messageAndOffset.message().payload();
      byte[] bytes = new byte[payload.limit()];
      payload.get(bytes);
      System.out.println(new String(bytes, "UTF-8"));
    }
  }

  public static void main(String[] args) throws Exception {
    String topic = args[0];

    SimpleConsumer simpleConsumer = new SimpleConsumer(KafkaProperties.kafkaServerURL,
                                                       9091,
                                                       KafkaProperties.connectionTimeOut,
                                                       KafkaProperties.kafkaProducerBufferSize,
                                                       KafkaProperties.clientId);

    FetchRequest req = new FetchRequestBuilder()
        .clientId(KafkaProperties.clientId)
        .addFetch(topic, 0, 0L, 10000)
        .build();
    FetchResponse fetchResponse = simpleConsumer.fetch(req);
    printMessages((ByteBufferMessageSet) fetchResponse.messageSet(topic, 0));
  }
}
