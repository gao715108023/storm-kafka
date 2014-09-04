package storm.kafka;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.RawMultiScheme;

import java.io.Serializable;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class KafkaConfig implements Serializable {

    //Kafka的IP地址和端口号或者Kafka映射到ZooKeeper的IP地址和端口号
    private BrokerHosts hosts;

    private int fetchSizeBytes = 1024 * 1024;

    //与Kafka连接的超时时间
    private int socketTimeoutMs = 10000;

    //缓存字节大小
    private int bufferSizeBytes = 1024 * 1024;

    //RawMultiScheme主要提供了两个方法，反序列化byte数组和获取Storm的Fields
    private MultiScheme scheme = new RawMultiScheme();

    //Kafka的Topic
    private String topic;

    private long startOffsetTime = -2;

    //强制重新开始
    private boolean forceFromStart = false;

    /**
     * 构造函数
     *
     * @param hosts
     * @param topic
     */
    public KafkaConfig(BrokerHosts hosts, String topic) {
        this.hosts = hosts;
        this.topic = topic;
    }

    public void forceStartOffsetTime(long millis) {
        startOffsetTime = millis;
        forceFromStart = true;
    }

    public BrokerHosts getHosts() {
        return hosts;
    }

    public String getTopic() {
        return topic;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public int getBufferSizeBytes() {
        return bufferSizeBytes;
    }

    public boolean isForceFromStart() {
        return forceFromStart;
    }

    public long getStartOffsetTime() {
        return startOffsetTime;
    }

    public int getFetchSizeBytes() {
        return fetchSizeBytes;
    }

    public MultiScheme getScheme() {
        return scheme;
    }
}
