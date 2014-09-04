package storm.kafka;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class ZkState {

    private static final Logger LOG = LoggerFactory.getLogger(ZkState.class);

    private CuratorFramework curator;

    public CuratorFramework getCurator() {
        return curator;
    }

    /**
     * 构造函数
     *
     * @param stateConf
     */
    public ZkState(Map stateConf) {

        //stateConf = new HashMap(stateConf);
        try {
            curator = newCurator(stateConf);
            curator.start(); //启动Zookeeper client
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建Zookeeper游标
     *
     * @param stateConf
     * @return
     * @throws java.io.IOException
     */
    private CuratorFramework newCurator(Map stateConf) throws IOException {
        Integer port = (Integer) stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_PORT);
        String serverPorts = "";
        for (String server : (List<String>) stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS)) {
            serverPorts = serverPorts + server + ":" + port + ",";
        }
        LOG.info("Zookeeper Address for Spout to store offset: " + serverPorts);
        return CuratorFrameworkFactory.newClient(serverPorts, Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)), 15000, new RetryNTimes(Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)), Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
    }

    public void writeJSON(String path, Map<Object, Object> data) {
        LOG.info("Writing " + path + " the data " + data.toString());
        writeBytes(path, JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8")));

    }

    private void writeBytes(String path, byte[] bytes) {
        try {
            if (curator.checkExists().forPath(path) == null) {
                curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, bytes);
            } else {
                curator.setData().forPath(path, bytes);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 从Zookeeper中读取信息
     *
     * @param path zookeeper的路径
     * @return
     */
    public Map<Object, Object> readJson(String path) {
        byte[] b = readBytes(path);
        if (b == null)
            return null;
        try {
            return (Map<Object, Object>) JSONValue.parse(new String(b, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据路径获取Zookeeper中的值
     *
     * @param path zookeeper的路径
     * @return
     */
    private byte[] readBytes(String path) {
        try {
            if (curator.checkExists().forPath(path) != null) {
                return curator.getData().forPath(path);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        curator.close();
        curator = null;
    }

}
