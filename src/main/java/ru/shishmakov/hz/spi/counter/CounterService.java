package ru.shishmakov.hz.spi.counter;

import com.google.common.collect.Range;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.BoundType.CLOSED;
import static com.google.common.collect.BoundType.OPEN;

/**
 * The {@link CounterService} is the gateway into the Hazelcast internals.
 * Through this gateway, you will be able to create proxies, participate in partition migration, and so on.
 *
 * @author Dmitriy Shishmakov on 19.10.16
 */
public class CounterService implements ManagedService, RemoteService, MigrationAwareService {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String NAME = "CounterService";
    private static final String CLASS_NAME = CounterService.class.getSimpleName();

    private CounterContainer[] containers = new CounterContainer[0];

    private NodeEngine nodeEngine;
    private Properties properties;

    public CounterService() {
        /* need to be */
    }

    /**
     * CounterService is initialized for every partition
     */
    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.properties = properties;
        CounterContainer[] containers = new CounterContainer[nodeEngine.getPartitionService().getPartitionCount()];
        for (int i = 0; i < containers.length; i++) {
            containers[i] = new CounterContainer();
        }
        this.containers = containers;
        logger.debug("Init {} with partitions CounterContainer[{}]", NAME, containers.length);
    }

    /**
     * Clean up resources
     */
    @Override
    public void shutdown(boolean terminate) {
        logger.debug("{} shutdown", CounterService.CLASS_NAME);
    }

    /**
     * Merge after the split-brain problem
     */
    @Override
    public void reset() {
        /* not implement */
        logger.debug("{} reset", CounterService.CLASS_NAME);
    }

    /**
     * Build a proxy is a local representation of (potentially) remote managed data and logic.
     * It is important to realize that caching the proxy instance and removing the proxy instance is done outside of this service.
     *
     * @return proxy instance
     */
    @Override
    public DistributedObject createDistributedObject(String objectId) {
        final int partitionId = nodeEngine.getPartitionService().getPartitionId(objectId);
        CounterContainer container = getContainerByPartitionId(partitionId);
        container.init(objectId);
        logger.debug("{} create proxy: {}", CounterService.CLASS_NAME, CounterProxy.CLASS_NAME);
        return new CounterProxy(objectId, nodeEngine, this);
    }

    /**
     * Clean up the value for the object has been removed.
     */
    @Override
    public void destroyDistributedObject(String objectId) {
        final int partitionId = nodeEngine.getPartitionService().getPartitionId(objectId);
        CounterContainer container = getContainerByPartitionId(partitionId);
        container.destroy(objectId);
        logger.debug("{} destroy proxy", CounterService.CLASS_NAME);
    }

    public CounterContainer getContainerByPartitionId(int partitionId) {
        checkArgument(Range.range(0, CLOSED, containers.length, OPEN).contains(partitionId), "partitionId: %s is illegal", partitionId);
        return containers[partitionId];
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        CounterContainer container = getContainerByPartitionId(partitionId);
        container.clear();
    }

    /**
     * Method creates operation migration and returns all the data in the partition that is going to be moved
     */
    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CounterContainer container = getContainerByPartitionId(event.getPartitionId());
        Map<String, Integer> migrationData = container.toMigrationData();
        return migrationData.isEmpty() ? null : new MigOperation(migrationData);
    }

    /**
     * Committing means that we clear the container for the partition of the old owner
     */
    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            CounterContainer container = getContainerByPartitionId(event.getPartitionId());
            container.clear();
            logger.debug("Commit migration data from partition: {}, endpoint: {}",
                    event.getPartitionId(), event.getMigrationEndpoint());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            CounterContainer container = getContainerByPartitionId(event.getPartitionId());
            container.clear();
            logger.debug("Rollback migration data from partition: {}, endpoint: {}",
                    event.getPartitionId(), event.getMigrationEndpoint());
        }
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        logger.debug("Before migration operation; partition: {}, endpoint: {}",
                event.getPartitionId(), event.getMigrationEndpoint());
    }
}
