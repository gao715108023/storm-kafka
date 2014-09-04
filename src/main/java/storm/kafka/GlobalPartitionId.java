package storm.kafka;

import storm.trident.spout.ISpoutPartition;

/**
 * Kafka的server和每个server的分区号
 *
 * @author gaochuanjun
 * @since 14-9-3
 */
public class GlobalPartitionId implements ISpoutPartition {

    private HostPort host;

    //分区号
    private int partition;

    public GlobalPartitionId(HostPort host, int partition) {
        this.host = host;
        this.partition = partition;
    }

    @Override
    public String getId() {
        return host.toString() + ":" + partition;
    }

    public HostPort getHost() {
        return host;
    }

    public int getPartition() {
        return partition;
    }
}
