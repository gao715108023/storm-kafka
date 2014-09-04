package storm.kafka;

import java.util.ArrayList;
import java.util.List;

/**
 * 存储的是Kafka的服务器地址和端口号以及每个Server上的分区数
 *
 * @author gaochuanjun
 * @since 14-9-3
 */
public class StaticHosts implements BrokerHosts {

    //Kafka的Server地址和端口号列表
    private List<HostPort> hosts;
    //每个server上的分区数
    private int partitionsPerHost;

    public StaticHosts fromHostString(List<String> hostStrings, int partitionsPerHost) {
        return new StaticHosts(convertHosts(hostStrings), partitionsPerHost);
    }

    public StaticHosts(List<HostPort> hosts, int partitionsPerHost) {
        this.hosts = hosts;
        this.partitionsPerHost = partitionsPerHost;
    }

    public static int getNumHosts(BrokerHosts hosts) {
        if (!(hosts instanceof StaticHosts)) {
            throw new RuntimeException("Must use static hosts");
        }
        return ((StaticHosts) hosts).hosts.size();
    }

    private List<HostPort> convertHosts(List<String> hosts) {

        List<HostPort> ret = new ArrayList<HostPort>();
        for (String s : hosts) {
            HostPort hp;
            String[] spec = s.split(":");
            if (spec.length == 1) {
                hp = new HostPort(spec[0]);
            } else if (spec.length == 2) {
                hp = new HostPort(spec[0], Integer.parseInt(spec[1]));
            } else {
                throw new IllegalArgumentException("Invalid host specification: " + s);
            }
            ret.add(hp);
        }

        return ret;
    }

    public List<HostPort> getHosts() {
        return hosts;
    }

    public int getPartitionsPerHost() {
        return partitionsPerHost;
    }
}
