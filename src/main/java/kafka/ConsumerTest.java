package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * Created by qmgeng on 2015/11/10.
 */
public class ConsumerTest {

  public static String ZK_CONNECT = "";
  public static String TOPIC = "";

  /**
   * 该consumer所属的组ID
   */
  private String groupid;

  /**
   * 该consumer的ID
   */
  private String consumerid;

  /**
   * 每个topic开几个线程？
   */
  private int threadPerTopic;

  public ConsumerTest(String groupid, String consumerid, int threadPerTopic) {
    super();
    this.groupid = groupid;
    this.consumerid = consumerid;
    this.threadPerTopic = threadPerTopic;
  }

  public void consume() {
    Properties props = new Properties();
    props.put("group.id", groupid);
    props.put("consumer.id", consumerid);
    props.put("zookeeper.connect", ZK_CONNECT);
    props.put("zookeeper.session.timeout.ms", "60000");
    props.put("zookeeper.sync.time.ms", "2000");
    // props.put("auto.commit.interval.ms", "1000");

    ConsumerConfig config = new ConsumerConfig(props);
    ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

    // 设置每个topic开几个线程
    topicCountMap.put(TOPIC, threadPerTopic);

    // 获取stream
    Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topicCountMap);

    // 为每个stream启动一个线程消费消息
    for (KafkaStream<byte[], byte[]> stream : streams.get(TOPIC)) {
      new MyStreamThread(stream).start();
    }
  }

  /**
   * 每个consumer的内部线程
   *
   * @author cuilei05
   *
   */
  private class MyStreamThread extends Thread {
    private KafkaStream<byte[], byte[]> stream;

    public MyStreamThread(KafkaStream<byte[], byte[]> stream) {
      super();
      this.stream = stream;
    }

    @Override
    public void run() {
      ConsumerIterator<byte[], byte[]> streamIterator = stream.iterator();

      // 逐条处理消息
      while (streamIterator.hasNext()) {
        MessageAndMetadata<byte[], byte[]> message = streamIterator.next();
        String topic = message.topic();
        int partition = message.partition();
        long offset = message.offset();
        String key = new String(message.key());
        String msg = new String(message.message());
        // 在这里处理消息,这里仅简单的输出
        // 如果消息消费失败，可以将已上信息打印到日志中，活着发送到报警短信和邮件中，以便后续处理
        System.out.println("consumerid:" + consumerid + ", thread : " + Thread.currentThread().getName() + ", topic : "
            + topic + ", partition : " + partition + ", offset : " + offset + " , key : " + key + " , mess : " + msg);
      }
    }
  }

  public static void main(String[] args) {
    String groupid = "myconsumergroup";
    ConsumerTest consumer1 = new ConsumerTest(groupid, "myconsumer1", 3);
    ConsumerTest consumer2 = new ConsumerTest(groupid, "myconsumer2", 3);

    consumer1.consume();
    consumer2.consume();
  }
}
