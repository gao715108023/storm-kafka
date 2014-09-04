package storm.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class StaticCoordinator implements PartitionCoordinator {

    Map<GlobalPartitionId, PartitionManager> managers = new HashMap<GlobalPartitionId, PartitionManager>();

    private List<PartitionManager> allManagers;


    /**
     * 构造函数
     *
     * @param connections
     * @param stormConf          Storm的配置
     * @param config             Spout的配置
     * @param state
     * @param taskIndex          task的index
     * @param totalTasks         task数目
     * @param topologyInstanceId 唯一序列号，随机生成
     */
    public StaticCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig config, ZkState state, int taskIndex, int totalTasks, String topologyInstanceId) {
        StaticHosts hosts = (StaticHosts) config.getHosts();
        List<GlobalPartitionId> allPartitionIds = new ArrayList<GlobalPartitionId>();
        for (HostPort h : hosts.getHosts()) {
            for (int i = 0; i < hosts.getPartitionsPerHost(); i++) {
                allPartitionIds.add(new GlobalPartitionId(h, i));
            }
        }

        for (int i = taskIndex; i < allPartitionIds.size(); i += totalTasks) {
            GlobalPartitionId myPartition = allPartitionIds.get(i);
            managers.put(myPartition, new PartitionManager(connections, topologyInstanceId, state, stormConf, config, myPartition));
        }
        allManagers = new ArrayList<PartitionManager>(managers.values());
    }

    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        return allManagers;
    }

    @Override
    public PartitionManager getManager(GlobalPartitionId id) {
        return managers.get(id);
    }
}
