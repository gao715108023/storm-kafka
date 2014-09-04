package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class DynamicBrokersReader {

    private String zkPath;

    private String topic;

    private CuratorFramework curator;

    /**
     * 构造函数
     *
     * @param conf   Storm的配置
     * @param zkStr  Kafka映射的Zookeeper的IP地址和端口号
     * @param zkPath Kafka映射的ZooKeeper中的路径
     * @param topic  Kafka中的topic
     */
    public DynamicBrokersReader(Map conf, String zkStr, String zkPath, String topic) {
        this.zkPath = zkPath;
        this.topic = topic;
        try {
            curator = CuratorFrameworkFactory.newClient(zkStr, Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)), 15000, new RetryNTimes(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)), Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
            curator.start();//启动Zookeeper的Client
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从Zookeeper中获取Kafka的信息
     *
     * @return key-Kafka的地址，value-[port,分区数]
     */
    public Map<String, List> getBrokerInfo() {

        Map<String, List> ret = new HashMap<>();

        String topicBrokersPath = zkPath + "/topics/" + topic; //kafka的topic在Zookeeper中的路径，该路径下存储的是分区数

        String brokerInfoPath = zkPath + "/ids";  //Kafk在Zookeeper中的路径，该路径下存数的是Kafka的具体server地址和端口号

        try {

            List<String> children = curator.getChildren().forPath(topicBrokersPath);//获取topicBrokersPath路径下的子节点

            //获取每个子节点下的具体值
            for (String c : children) {
                byte[] numPartitionsData = curator.getData().forPath(topicBrokersPath + "/" + c);
                byte[] hostPortData = curator.getData().forPath(brokerInfoPath + "/" + c);

                HostPort hp = getBrokerHost(hostPortData);

                int numPartitions = getNumPartitions(numPartitionsData);

                List<Long> info = new ArrayList<>();
                info.add((long) hp.getPort());
                info.add((long) numPartitions);
                ret.put(hp.getHost(), info);

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    public void close() {
        curator.close();
    }


    /**
     * 根据Zookeeper中的值获取Kafka的Server地址和端口号
     *
     * @param contents
     * @return
     */
    private HostPort getBrokerHost(byte[] contents) {
        try {
            String[] hostString = new String(contents, "UTF-8").split(":");
            String host = hostString[hostString.length - 2];
            int port = Integer.parseInt(hostString[hostString.length - 1]);
            return new HostPort(host, port);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据Zookeeper中的值获取Kafka中的分区数
     *
     * @param contents
     * @return
     */
    private int getNumPartitions(byte[] contents) {
        try {
            return Integer.parseInt(new String(contents, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException();
        }
    }
}
