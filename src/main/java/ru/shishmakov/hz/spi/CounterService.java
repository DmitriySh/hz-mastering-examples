package ru.shishmakov.hz.spi;

import com.google.common.collect.Range;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author Dmitriy Shishmakov on 19.10.16
 */
public class CounterService implements ManagedService, RemoteService {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String NAME = "CounterService";
    static final String CLASS_NAME = CounterService.class.getSimpleName();
    private CounterContainer[] containers = new CounterContainer[0];

    private NodeEngine nodeEngine;
    private Properties properties;

    public CounterService() {
        /* need to be */
    }

    /**
     * CounterService is initialized
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
        System.out.println("{} init");
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

    @Override
    public DistributedObject createDistributedObject(String objectId) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(objectId);
        CounterContainer container = containers[partitionId];
        container.init(objectId);
        logger.debug("{} create proxy: {}", CounterService.CLASS_NAME, CounterProxy.CLASS_NAME);
        return new CounterProxy(objectId, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectId) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(objectId);
        CounterContainer container = containers[partitionId];
        container.destroy(objectId);
        logger.debug("{} destroy proxy", CounterService.CLASS_NAME);
    }

    public CounterContainer getContainerByPartitionId(int partitionId) {
        checkArgument(Range.open(0, containers.length).contains(partitionId), "partitionId: %s is illegal");
        return containers[partitionId];
    }
}
