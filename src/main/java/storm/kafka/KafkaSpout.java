package storm.kafka;

import backtype.storm.Config;
import backtype.storm.metric.api.IMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.vip.fds.storm.Field;
import com.vip.fds.storm.kafka.trident.KafkaOffsetMetric;

import java.util.*;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class KafkaSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private SpoutConfig spoutConfig;
    private ZkState zkState;
    private DynamicPartitionConnections connections;

    private PartitionCoordinator coordinator;//协调分区接口，直接连接Kafka与连接Zookeeper的处理方式是不一样的

    private String uuid = UUID.randomUUID().toString();

    private int currPartitionIndex = 0;

    private long lastUpdateMs = 0;

    /**
     * 构造函数，传入Storm的Spout的参数和Kafka的参数
     *
     * @param spoutConfig
     */
    public KafkaSpout(SpoutConfig spoutConfig) {
        this.spoutConfig = spoutConfig;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(spoutConfig.getScheme().getOutputFields());
        declarer.declare(new Fields(Field.KAFKASPOUTFIELD));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        Map stateConf = new HashMap(conf);
        List<String> zkServers = spoutConfig.getZkServers();

        //如果zkServers为空，则使用Storm的Zookeeper地址
        if (zkServers == null)
            zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);

        Integer zkPort = spoutConfig.getZkPort();

        //如果zkPort为空，则默认使用Storm的Zookeeper端口号
        if (zkPort == null)
            zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();

        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);

        //ZooKeeper的根路径
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, spoutConfig.getZkRoot());

        zkState = new ZkState(stateConf);

        connections = new DynamicPartitionConnections(spoutConfig);

        // 获取所有的task数目
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();

        if (spoutConfig.getHosts() instanceof StaticHosts) { //如果实例化为StaticHosts
            coordinator = new StaticCoordinator(connections, conf, spoutConfig, zkState, context.getThisTaskIndex(), totalTasks, uuid);
        } else { //1. 如果实例化为 ZkHosts
            // 2. 获取该task的index
            coordinator = new ZkCoordinator(connections, conf, spoutConfig, zkState, context.getThisTaskIndex(), totalTasks, uuid);
        }

        context.registerMetric("kafkaOffset", new IMetric() {

            KafkaOffsetMetric kafkaOffsetMetric = new KafkaOffsetMetric(spoutConfig.getTopic(), connections);

            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = coordinator.getMyManagedPartitions();
                Set<GlobalPartitionId> lastestPartitions = new HashSet<>();
                for (PartitionManager pm : pms) {

                    lastestPartitions.add(pm.getPartition());
                }
                kafkaOffsetMetric.refreshPartitions(lastestPartitions);

                for (PartitionManager pm : pms) {
                    kafkaOffsetMetric.setLatestEmittedOffset(pm.getPartition(), pm.lastCommittedOffset());
                }
                return kafkaOffsetMetric.getValueAndReset();
            }
        }, 60);

        context.registerMetric("kafkaPartition", new IMetric() {
            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = coordinator.getMyManagedPartitions();
                Map concatMetricsDataMaps = new HashMap();
                for (PartitionManager pm : pms) {
                    concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
                }
                return concatMetricsDataMaps;
            }
        }, 60);
    }

    @Override
    public void nextTuple() {
        List<PartitionManager> managers = coordinator.getMyManagedPartitions();
        for (int i = 0; i < managers.size(); i++) {
            currPartitionIndex = currPartitionIndex % managers.size();
            EmitState state = managers.get(currPartitionIndex).next(collector);
            if (state != EmitState.EMITTED_MORE_LEFT) {
                currPartitionIndex = (currPartitionIndex + 1) % managers.size();
            }
            if (state != EmitState.NO_EMITTED) {
                break;
            }
        }

        long now = System.currentTimeMillis();

        if ((now - lastUpdateMs) > spoutConfig.getStateUpdateIntervalMs()) {
            commit();
        }
    }


    @Override
    public void close() {
        zkState.close();
    }

    @Override
    public void ack(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        PartitionManager m = coordinator.getManager(id.getPartition());
        if (m != null) {
            m.fail(id.getOffset());
        }
    }

    @Override
    public void fail(Object magId) {
        KafkaMessageId id = (KafkaMessageId) magId;
        PartitionManager m = coordinator.getManager(id.getPartition());

        if (m != null) {
            m.fail(id.getOffset());
        }
    }

    @Override
    public void deactivate() {
        commit();
    }

    private void commit() {
        lastUpdateMs = System.currentTimeMillis();
        for (PartitionManager manager : coordinator.getMyManagedPartitions()) {
            manager.commit();
        }
    }


}
