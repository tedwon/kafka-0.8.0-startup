package kafka.examples.consumer;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.api.OffsetRequest;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * <p/>
 *
 * @author <a href=mailto:iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class MultipartitionHighLevelConsumerAPIDemo {

  private ExecutorService executor_;
  private final ConsumerConnector consumer_;
  private final String topic_;

  private final int numPartitions_ = 4;

  public MultipartitionHighLevelConsumerAPIDemo(String zookeeper, String groupId, String topic) {
    Properties props = new Properties();
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", groupId);
    props.put("zookeeper.session.timeout.ms", "5000");
    props.put("zookeeper.sync.time.ms", "250");
    props.put("auto.commit.interval.ms", "1000");
//    props.put("auto.offset.reset", OffsetRequest.SmallestTimeString());
    props.put("auto.offset.reset", OffsetRequest.LargestTimeString());

    consumer_ = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    topic_ = topic;
  }

  public void run() throws Exception {
    run(numPartitions_);
  }

  public void run(int numPartitions) throws Exception {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic_, new Integer(numPartitions));

    Map<String, List<KafkaStream<byte[], byte[]>>>
        consumerMap =
        consumer_.createMessageStreams(topicCountMap);

    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic_);

    // Launching the thread pool
    executor_ = Executors.newFixedThreadPool(numPartitions_);

    //Creating an object messages consumption
    int threadNumber = 0;
    for (final KafkaStream<byte[], byte[]> stream : streams) {
//      executor_.submit(new ConsumerTest(stream, threadNumber));

      executor_.submit(new Runnable() {
        @Override
        public void run() {
//          for(MessageAndMetadata msgAndMetadata: stream) {
//            System.out.println(1);
//          }
          ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
          while (iterator.hasNext()) {
            MessageAndMetadata<byte[], byte[]> next = iterator.next();
            long offset = next.offset();
            int partition = next.partition();
//            System.out.println(partition);

            byte[] keys = next.key();
            String key = "null";
            if (keys != null) {
              try {
                key = new String(keys, "UTF-8");
              } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
              }
            }

            byte[] bytes = next.message();
            String message = null;
            try {
              message = new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
              e.printStackTrace();
            }
            System.out.println(
                "partition=" + partition + ", offset=" + offset + ", key=" + key + ", value="
                + message);

//        Thread.sleep(1000);
          }
        }
      });

      threadNumber++;
    }
//
//    if (consumer_ != null) {
//      consumer_.shutdown();
//    }
//    if (executor_ != null) {
//      executor_.shutdown();
//    }
  }
}
