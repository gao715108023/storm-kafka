package storm.kafka;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public class KafkaMessageId {

    private GlobalPartitionId partition;

    private long offset;

    public KafkaMessageId(GlobalPartitionId partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public GlobalPartitionId getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
}
