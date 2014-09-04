package storm.kafka;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class ZkHosts implements BrokerHosts {

    //Kafka映射的Zookeeper的IP地址和端口号
    private String brokerZkstr;

    //Kafka映射的ZooKeeper中的路径
    private String brokerZkPath;

    private int refreshFreqSecs = 60;

    public ZkHosts(String brokerZkstr, String brokerZkPath) {
        this.brokerZkstr = brokerZkstr;
        this.brokerZkPath = brokerZkPath;
    }

    public int getRefreshFreqSecs() {
        return refreshFreqSecs;
    }

    public String getBrokerZkstr() {
        return brokerZkstr;
    }

    public String getBrokerZkPath() {
        return brokerZkPath;
    }
}
