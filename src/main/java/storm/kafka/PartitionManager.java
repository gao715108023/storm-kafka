package storm.kafka;


import backtype.storm.Config;
import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;
import com.google.common.collect.ImmutableMap;
import com.vip.fds.storm.kafka.trident.MaxMetric;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class PartitionManager {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    private DynamicPartitionConnections connections;

    private String topologyInstanceId;

    private ZkState state;

    private Map stormConf;

    private SpoutConfig spoutConfig;

    private GlobalPartitionId partition;

    private Long emittedToOffset;

    private SortedSet<Long> pending = new TreeSet<Long>();

    private Long committedTo;

    private LinkedList<MessageAndRealOffset> waitingToEmit = new LinkedList<MessageAndRealOffset>();
    //Kafka的消费者
    private SimpleConsumer consumer;

    private final CombinedMetric fetchAPILatencyMax;

    private final ReducedMetric fetchAPILatencyMean;

    private final CountMetric fetchAPICallCount;

    private final CountMetric fetchAPIMessageCount;

    /**
     * 构造函数
     *
     * @param connections        动态partition连接信息
     * @param topologyInstanceId 唯一序列号，随机生成
     * @param state              Zk状态信息
     * @param stormConf          storm的配置文件
     * @param spoutConfig        spout的配置文件
     * @param id                 partition id
     */
    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, ZkState state, Map stormConf, SpoutConfig spoutConfig, GlobalPartitionId id) {
        this.connections = connections;
        this.topologyInstanceId = topologyInstanceId;
        this.state = state;
        this.stormConf = stormConf;
        this.spoutConfig = spoutConfig;
        this.partition = id;

        //注册该分区
        consumer = connections.register(id.getHost(), id.getPartition());

        String jsonTopologyId = null;
        Long jsonOffset = null;

        Map<Object, Object> json = state.readJson(committedPath());

        if (json != null) {
            jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
            jsonOffset = (Long) json.get("offset");
        }

        //获取当前offset值
        if (topologyInstanceId.equals(jsonTopologyId) && spoutConfig.isForceFromStart()) {
            committedTo = consumer.getOffsetsBefore(spoutConfig.getTopic(), id.getPartition(), spoutConfig.getStartOffsetTime(), 1)[0];
            LOG.info("Setting last commit offset to HEAD.");
        } else if (jsonTopologyId == null || jsonOffset == null) {
            committedTo = consumer.getOffsetsBefore(spoutConfig.getTopic(), id.getPartition(), -1, 1)[0];
        } else {
            committedTo = jsonOffset;
            LOG.info("Read last commit offset to HEAD.");
        }

        LOG.info("Starting Kafka " + consumer.host() + ":" + id.getPartition() + " from offset " + committedTo);
        emittedToOffset = committedTo;

        fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
        fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
        fetchAPICallCount = new CountMetric();
        fetchAPIMessageCount = new CountMetric();
    }

    public Map getMetricsDataMap() {
        Map ret = new HashMap();
        ret.put(partition + "/fetchAPILatencyMax", fetchAPILatencyMax.getValueAndReset());
        ret.put(partition + "/fetchAPILatencyMean", fetchAPILatencyMean.getValueAndReset());
        ret.put(partition + "/fetchAPICallCount", fetchAPICallCount.getValueAndReset());
        ret.put(partition + "/fetchAPIMessageCount", fetchAPIMessageCount.getValueAndReset());
        return ret;
    }

    public EmitState next(SpoutOutputCollector collector) {
        if (waitingToEmit.isEmpty())
            fill();
        while (true) {
            MessageAndRealOffset toEmit = waitingToEmit.pollFirst();
            if (toEmit == null) {
                return EmitState.NO_EMITTED;
            }
            Iterable<List<Object>> tups = spoutConfig.getScheme().deserialize(Utils.toByteArray(toEmit.getMsg().payload()));

            if (tups != null) {
                for (List<Object> tup : tups)
                    collector.emit(tup, new KafkaMessageId(partition, toEmit.getOffset()));
                break;
            } else {
                ack(toEmit.getOffset());
            }
        }
        if (!waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
    }

    public void commit() {
        LOG.info("Committing offset for " + partition);
        long committedTo;
        if (pending.isEmpty()) {
            committedTo = emittedToOffset;
        } else {
            committedTo = pending.first();
        }

        if (committedTo != this.committedTo) {
            LOG.info("Writing committed offset to ZK: " + committedTo);

            Map<Object, Object> data = ImmutableMap.builder()
                    .put("topology", ImmutableMap.of("id", topologyInstanceId, "name", stormConf.get(Config.TOPOLOGY_NAME)))
                    .put("offset", committedTo)
                    .put("partition", partition.getPartition())
                    .put("broker", ImmutableMap.of("host", partition.getHost().getHost(), "port", partition.getHost().getPort()))
                    .put("topic", spoutConfig.getTopic()).build();

            state.writeJSON(committedPath(), data);

            LOG.info("Wrote committed offset to ZK: " + committedTo);
            this.committedTo = committedTo;
        }
        LOG.info("Committed offset " + committedTo + " for " + partition);
    }

    public void fail(Long offset) {
        if (emittedToOffset > offset) {
            emittedToOffset = offset;
            pending.tailSet(offset).clear();
        }
    }

    private void fill() {
        long start = System.nanoTime();
        ByteBufferMessageSet msgs = consumer.fetch(new FetchRequest(spoutConfig.getTopic(), partition.getPartition(), emittedToOffset, spoutConfig.getFetchSizeBytes()));
        long end = System.nanoTime();
        long millis = (end - start) / 1000000;
        fetchAPILatencyMax.update(millis);
        fetchAPILatencyMean.update(millis);
        fetchAPICallCount.incr();
        fetchAPIMessageCount.incrBy(msgs.underlying().size());

        int numMessages = msgs.underlying().size();
        if (numMessages > 0) {
            LOG.info("Fetched " + numMessages + " messages from Kafka: " + consumer.host() + ":" + partition.getPartition());
        }

        for (MessageAndOffset msg : msgs) {
            pending.add(emittedToOffset);
            waitingToEmit.add(new MessageAndRealOffset(msg.message(), emittedToOffset));
            emittedToOffset = msg.offset();
        }
        if (numMessages > 0) {
            LOG.info("Added " + numMessages + " message from Kafka: " + consumer.host() + ":" + partition.getPartition() + " to internal buffers");
        }
    }

    private void ack(Long offset) {
        pending.remove(offset);
    }

    /**
     * Zookeeper的地址，存储格式为root/topology名称/partition id
     *
     * @return
     */
    private String committedPath() {
        return spoutConfig.getZkRoot() + "/" + spoutConfig.getId() + "/" + partition;
    }

    /**
     * 关闭连接
     */
    public void close() {
        connections.unregister(partition.getHost(), partition.getPartition());
    }


    public GlobalPartitionId getPartition() {
        return partition;
    }

    public long lastCommittedOffset() {
        return committedTo;
    }
}
