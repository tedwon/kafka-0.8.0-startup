package kafka.examples.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * <p/>
 *
 * @author <a href=mailto:iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class ConsumerTest implements Runnable {

  KafkaStream stream_;
  int threadNumber_;

  public ConsumerTest(KafkaStream stream, int threadNumber) {
    stream_ = stream;
    threadNumber_ = threadNumber;
  }

  @Override
  public void run() {
    ConsumerIterator<byte[], byte[]> iterator = stream_.iterator();
    while (iterator.hasNext()) {
      try {
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

//        Thread.sleep(1000);
      } catch (Exception e) {
      }
    }
  }
}
