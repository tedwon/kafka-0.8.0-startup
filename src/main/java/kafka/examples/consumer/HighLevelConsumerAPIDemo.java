package kafka.examples.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.api.OffsetRequest;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * The high-level consumer API Demo Class.<p/>
 *
 * See https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example
 *
 * @author <a href=mailto:iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class HighLevelConsumerAPIDemo {

  private final ConsumerConnector consumer_;
  private final String topic_;

  private final int numThreads_ = 1;

  public HighLevelConsumerAPIDemo(String zookeeper, String groupId, String topic) {
    Properties props = new Properties();
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", groupId);
    props.put("zookeeper.session.timeout.ms", "5000");
    props.put("zookeeper.sync.time.ms", "250");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", OffsetRequest.SmallestTimeString());

    consumer_ = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    topic_ = topic;
  }

  public void run() throws Exception {
    run(numThreads_);
  }

  public void run(int numThreads) throws Exception {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic_, new Integer(numThreads));

    Map<String, List<KafkaStream<byte[], byte[]>>>
        consumerMap =
        consumer_.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic_);
    for (final KafkaStream<byte[], byte[]> stream : streams) {
      ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
      while (iterator.hasNext()) {
        MessageAndMetadata<byte[], byte[]> next = iterator.next();
        long offset = next.offset();
        int partition = next.partition();

        byte[] keys = next.key();
        String key = "null";
        if (keys != null) {
          key = new String(keys, "UTF-8");
        }

        byte[] bytes = next.message();
        String message = new String(bytes, "UTF-8");
        System.out.println(
            "partition=" + partition + ", offset=" + offset + ", key=" + key + ", value="
            + message);

        Thread.sleep(1000);
      }
    }

    if (consumer_ != null) {
      consumer_.shutdown();
    }
  }
}