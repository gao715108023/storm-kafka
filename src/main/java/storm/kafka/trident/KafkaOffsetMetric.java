package storm.kafka.trident;


import backtype.storm.metric.api.IMetric;
import kafka.api.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.DynamicPartitionConnections;
import storm.kafka.GlobalPartitionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class KafkaOffsetMetric implements IMetric {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetMetric.class);

    private String topic;

    private DynamicPartitionConnections connections;

    private Set<GlobalPartitionId> partitions;

    private Map<GlobalPartitionId, Long> partitionToOffSet = new HashMap<GlobalPartitionId, Long>();

    /**
     * 构造函数
     *
     * @param topic
     * @param connections
     */
    public KafkaOffsetMetric(String topic, DynamicPartitionConnections connections) {
        this.topic = topic;
        this.connections = connections;
    }

    /**
     * 取得当前值并恢复初始状态
     *
     * @return
     */
    @Override
    public Object getValueAndReset() {

        long totalSpoutLag = 0;
        long totalLatestTimeOffset = 0;
        long totalLatestEmittedOffset = 0;
        HashMap ret = new HashMap();
        if (partitions != null && partitions.size() == partitionToOffSet.size()) {
            for (Map.Entry<GlobalPartitionId, Long> e : partitionToOffSet.entrySet()) {
                GlobalPartitionId partition = e.getKey();
                SimpleConsumer consumer = connections.getConnection(partition);
                if (consumer == null) {
                    LOG.warn("partitionToOffset contains partition not found in connections, Stale partition data?");
                    return null;
                }
                long latestTimeOffset = consumer.getOffsetsBefore(topic, partition.getPartition(), OffsetRequest.LatestTime(), 1)[0];
                if (latestTimeOffset == 0) {
                    LOG.warn("No data found in Kafka Partition " + partition.getId());
                    return null;
                }
                long latestEmittedOffset = e.getValue();
                long spoutLag = latestTimeOffset - latestEmittedOffset;
                ret.put(partition.getId() + "/" + "spoutLag", spoutLag);
                ret.put(partition.getId() + "/" + "latestTime", latestEmittedOffset);
                ret.put(partition.getId() + "/" + "latestEmittedOffset", latestEmittedOffset);
                totalSpoutLag += spoutLag;
                totalLatestTimeOffset += latestTimeOffset;
                totalLatestEmittedOffset += latestEmittedOffset;
            }
            ret.put("totalSpoutLag", totalSpoutLag);
            ret.put("totalLatestTime", totalLatestTimeOffset);
            ret.put("totalLatestEmittedOffset", totalLatestEmittedOffset);
            return ret;
        } else {
            LOG.info("Metrics Tick: Not enough data to calculate spout lag.");
        }
        return null;
    }

    public void setLatestEmittedOffset(GlobalPartitionId partition, long offset) {
        partitionToOffSet.put(partition, offset);
    }

    public void refreshPartitions(Set<GlobalPartitionId> partitions) {
        this.partitions = partitions;
        Iterator<GlobalPartitionId> it = partitionToOffSet.keySet().iterator();
        while (it.hasNext()) {
            if (!partitions.contains(it.next()))
                it.remove();
        }
    }

}
