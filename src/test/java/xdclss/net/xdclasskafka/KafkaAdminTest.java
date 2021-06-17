package xdclss.net.xdclasskafka;

import org.apache.kafka.clients.admin.*;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author: han
 * @Date： 2021/6/15 9:14 下午
 */
public class KafkaAdminTest {

    public static final String TOPIC_NAME = "xdclss-sp-topic-test";
    /**
     * 设置admin 客户端
     * @return
     */
    public static AdminClient initAdminClient(){
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"39.107.75.93:9092,39.107.75.93:9093,39.107.75.93:9094");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    //创建
    @Test
    public  void createTopic() {
        AdminClient adminClient = initAdminClient();
        // 2个分区，1个副本
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 6 , (short) 3);

        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));
        //future等待创建，成功不会有任何报错，如果创建失败和超时会报错。
        try {
            createTopicsResult.all().get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("创建新的topic");
    }

    //获取
    @Test
    public  void listTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = initAdminClient();
        //是否查看内部的topic,可以不用
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult listTopics = adminClient.listTopics(options);
        Set<String> topics = listTopics.names().get();
        for (String topic : topics) {
            System.err.println(topic);
        }
    }


    //删除
    @Test
    public  void delTopicTest() {
        AdminClient adminClient = initAdminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        try {
            deleteTopicsResult.all().get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //获取指定topic的详细信息
    @Test
    public  void getTopicInfo() throws Exception {
        AdminClient adminClient = initAdminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPIC_NAME));

        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();

        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();

        entries.stream().forEach((entry)-> System.out.println("name ："+entry.getKey()+" , desc: "+ entry.getValue()));
    }


    /**
     * 增加分区数量
     *
     * 如果当主题中的消息包含有key时(即key不为null)，根据key来计算分区的行为就会有所影响消息顺序性
     *
     * 注意：Kafka中的分区数只能增加不能减少，减少的话数据不知怎么处理
     *
     * @throws Exception
     */
    @Test
    public  void incrPartitionsTest() throws Exception{
        Map<String, NewPartitions> infoMap = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        AdminClient adminClient = initAdminClient();
        infoMap.put(TOPIC_NAME, newPartitions);
        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(infoMap);
        createPartitionsResult.all().get();
    }
}
