package storm.kafka;

import kafka.javaapi.consumer.SimpleConsumer;

import java.util.HashSet;
import java.util.Set;

/**
 * 维护Kafka的连接信息
 *
 * @author gaochuanjun
 * @since 14-9-3
 */
public class ConnectionInfo {

    private SimpleConsumer consumer;

    //partition id集合
    private Set<Integer> partitions = new HashSet<>();

    /**
     * 构造函数
     *
     * @param consumer 消费者
     */
    public ConnectionInfo(SimpleConsumer consumer) {
        this.consumer = consumer;
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }

    public SimpleConsumer getConsumer() {
        return consumer;
    }
}
