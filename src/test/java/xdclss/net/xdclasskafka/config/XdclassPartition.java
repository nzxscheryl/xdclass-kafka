package xdclss.net.xdclasskafka.config;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * @author: han
 * @Date： 2021/6/16 11:19 下午
 */
public class XdclassPartition implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if(keyBytes == null){
            throw new IllegalArgumentException("key 不能为null");
        }
        if("xdclass".equals(key)) {
            return 0;
        }
        //使用hash值取模，确定分区(默认的也是这个方式)
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
