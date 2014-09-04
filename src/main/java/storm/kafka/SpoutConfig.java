package storm.kafka;

import java.util.List;

/**
 * 继承父类Kafka的配置
 *
 * @author gaochuanjun
 * @since 14-9-3
 */
public class SpoutConfig extends KafkaConfig {

    //存储Zookeeper的IP地址，此ZooKeeper是为了存储offset的，可以与Storm的ZooKeeper和Kafka的Zookeepr不一样
    private List<String> zkServers;
    //ZooKeeper的端口号
    private Integer zkPort;
    //ZooKeeper的根路径
    private String zkRoot;
    //id一般为Topology名字
    private String id;
    private long stateUpdateIntervalMs = 2000;

    public SpoutConfig(BrokerHosts hosts, String topic, String zkRoot, String id, List<String> zkServers, Integer zkPort) {
        super(hosts, topic);
        this.zkRoot = zkRoot;
        this.id = id;
        this.zkServers = zkServers;
        this.zkPort = zkPort;
    }

    public List<String> getZkServers() {
        return zkServers;
    }

    public Integer getZkPort() {
        return zkPort;
    }

    public String getZkRoot() {
        return zkRoot;
    }

    public String getId() {
        return id;
    }

    public long getStateUpdateIntervalMs() {
        return stateUpdateIntervalMs;
    }
}
