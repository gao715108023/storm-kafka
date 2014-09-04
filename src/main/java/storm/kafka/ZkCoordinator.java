package storm.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class ZkCoordinator implements PartitionCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(ZkCoordinator.class);

    private DynamicPartitionConnections connections;

    private Map stormConf;

    private SpoutConfig spoutConfig;

    private ZkState state;

    private int taskIndex;

    private int totalTasks;
    //唯一序列号，随机生成
    private String topologyInstancesId;

    //刷新频率，默认为60秒
    private int refreshFreqMs;

    private DynamicBrokersReader reader;

    //上次刷新时间
    private Long lastRefreshTime;

    private List<PartitionManager> cachedList;

    private Map<GlobalPartitionId, PartitionManager> managers = new HashMap<>();

    /**
     * 构造函数
     *
     * @param connections
     * @param stormConf           Storm的配置
     * @param spoutConfig         Spout的配置
     * @param state
     * @param taskIndex           task的index
     * @param totalTasks          task数目
     * @param topologyInstancesId 唯一序列号，随机生成
     */
    public ZkCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig spoutConfig, ZkState state, int taskIndex, int totalTasks, String topologyInstancesId) {

        this.connections = connections;
        this.stormConf = stormConf;
        this.spoutConfig = spoutConfig;
        this.state = state;
        this.taskIndex = taskIndex;
        this.totalTasks = totalTasks;
        this.topologyInstancesId = topologyInstancesId;

        ZkHosts brokerConf = (ZkHosts) spoutConfig.getHosts();

        refreshFreqMs = brokerConf.getRefreshFreqSecs() * 1000;

        reader = new DynamicBrokersReader(stormConf, brokerConf.getBrokerZkstr(), brokerConf.getBrokerZkPath(), spoutConfig.getTopic());

    }

    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        //如果从未刷新过或者达到了刷新时间，则进行刷新
        if (lastRefreshTime == null || (System.currentTimeMillis() - lastRefreshTime) > refreshFreqMs) {
            refresh();
            lastRefreshTime = System.currentTimeMillis();
        }
        return cachedList;
    }

    @Override
    public PartitionManager getManager(GlobalPartitionId id) {
        return managers.get(id);
    }

    /**
     * 刷新
     */
    private void refresh() {

        LOG.info("Refreshing partition manager connections......");

        //从Zookeeper中获取Kafka的信息，key-kafka的server地址，value-[port,分区数]
        Map<String, List> brokerInfo = reader.getBrokerInfo();

        Set<GlobalPartitionId> mine = new HashSet<>();

        for (String host : brokerInfo.keySet()) {
            List info = brokerInfo.get(host);
            long port = (Long) info.get(0);

            HostPort hp = new HostPort(host, (int) port);
            long numPartitions = (Long) info.get(1);

            for (int i = 0; i < numPartitions; i++) {
                GlobalPartitionId id = new GlobalPartitionId(hp, i); //每个server映射一个partitionId
                if (myOwnership(id)) { //如果跟本task的index一样，则加入Set
                    mine.add(id);
                }
            }
        }

        Set<GlobalPartitionId> curr = managers.keySet();
        Set<GlobalPartitionId> newPartitions = new HashSet<>(mine);
        newPartitions.removeAll(curr); //移除当前的GlobalPartitionId

        Set<GlobalPartitionId> deletedPartitions = new HashSet<>(curr);
        deletedPartitions.removeAll(mine);

        LOG.info("Deleted partition managers: " + deletedPartitions.toString());

        for (GlobalPartitionId id : deletedPartitions) {
            PartitionManager man = managers.remove(id);  //从map中移出GlobalPartitionId
            man.close();
        }

        LOG.info("New partition managers: " + newPartitions.toString());

        for (GlobalPartitionId id : newPartitions) {
            PartitionManager man = new PartitionManager(connections, topologyInstancesId, state, stormConf, spoutConfig, id);
            managers.put(id, man);
        }

        cachedList = new ArrayList<>(managers.values());
        LOG.info("Finished refreshing");
    }

    /**
     * 判断该partitionId是否与taskIndex映射
     *
     * @param id Kafka的Server地址与partitionId
     * @return
     */
    private boolean myOwnership(GlobalPartitionId id) {
        int val = Math.abs(id.getHost().hashCode() + 23 * id.getPartition());
        return val % totalTasks == taskIndex;
    }
}
