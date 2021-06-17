package xdclss.net.xdclasskafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author: han
 * @Date： 2021/6/17 6:31 上午
 */
public class KafkaCondumerTest {


    public static Properties getProperties() {
        Properties props = new Properties();

        //broker地址
        props.put("bootstrap.servers", "39.107.75.93:9092");

        //消费者分组ID，分组内的消费者只能消费该消息一次，不同分组内的消费者可以重复消费该消息
        props.put("group.id", "xdclass");

        //开启自动提交offset
        props.put("enable.auto.commit", "true");

        //自动提交offset延迟时间
        props.put("auto.commit.interval.ms", "1000");

        //反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

    @Test
    public void simpleConsumerTest(){
        Properties props = getProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //订阅topic主题
        consumer.subscribe(Arrays.asList(KafkaAdminTest.TOPIC_NAME));

        while (true) {
            //拉取时间控制，阻塞超时时间
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                System.err.printf("topic = %s, offset = %d, key = %s, value = %s%n",record.topic(), record.offset(), record.key(), record.value());
            }
        }
    }
}
