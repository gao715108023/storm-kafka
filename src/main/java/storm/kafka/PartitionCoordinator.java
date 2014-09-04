package storm.kafka;

import java.util.List;

/**
 * @author gaochuanjun
 * @since 14-9-3
 */
public interface PartitionCoordinator {

    List<PartitionManager> getMyManagedPartitions();

    PartitionManager getManager(GlobalPartitionId id);
}
