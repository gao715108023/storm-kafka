package storm.kafka;

import kafka.javaapi.consumer.SimpleConsumer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class DynamicPartitionConnections {

    private KafkaConfig kafkaConfig;

    //维护与每个Kafka的server的连接信息
    private Map<HostPort, ConnectionInfo> connections = new HashMap<>();

    /**
     * 构造函数
     *
     * @param kafkaConfig kafka的配置文件
     */
    public DynamicPartitionConnections(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public SimpleConsumer register(GlobalPartitionId id) {
        return register(id.getHost(), id.getPartition());
    }


    /**
     * 记录该partition信息
     *
     * @param host      Kafka的server地址
     * @param partition partition
     * @return
     */
    public SimpleConsumer register(HostPort host, int partition) {
        if (!connections.containsKey(host)) {  //判断Map中是否已存在该连接信息
            connections.put(host, new ConnectionInfo(new SimpleConsumer(host.getHost(), host.getPort(), kafkaConfig.getSocketTimeoutMs(), kafkaConfig.getBufferSizeBytes())));
        }
        ConnectionInfo info = connections.get(host);
        info.getPartitions().add(partition);
        return info.getConsumer();
    }

    /**
     * 与Kafka断开连接
     *
     * @param port      Kafka的server地址
     * @param partition Kafka的partition id
     */
    public void unregister(HostPort port, int partition) {
        ConnectionInfo info = connections.get(port); //根据Kafka的server地址获取连接信息
        info.getPartitions().remove(partition);     //移除partition
        if (info.getPartitions().isEmpty()) {   //如果这是最后一个partition
            info.getConsumer().close();   //与Kafka断开连接
            connections.remove(port);    //从Map中移除该Kafka连接信息
        }
    }

    public SimpleConsumer getConnection(GlobalPartitionId id) {
        ConnectionInfo info = connections.get(id.getHost());
        if (info != null)
            info.getConsumer();
        return null;
    }

}
