package ru.shishmakov.hz.spi;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import static ru.shishmakov.hz.spi.CounterService.NAME;

/**
 * @author Dmitriy Shishmakov on 19.10.16
 */
public class CounterProxy extends AbstractDistributedObject<CounterService> implements Counter {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    static final String CLASS_NAME = CounterProxy.class.getSimpleName();

    private final String objectId;

    public CounterProxy(String objectId, NodeEngine nodeEngine, CounterService service) {
        super(nodeEngine, service);
        this.objectId = objectId;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getServiceName() {
        return objectId;
    }

    @Override
    public int increment(int delta) {
        NodeEngine engine = getNodeEngine();
        final int partitionId = engine.getPartitionService().getPartitionId(objectId);
        IncOperation operation = new IncOperation(objectId, delta);

        InvocationBuilder builder = engine.getOperationService().createInvocationBuilder(NAME, operation, partitionId);
        ICompletableFuture<Integer> future = builder.invoke();
        try {
            return future.get();
        } catch (Exception e) {
            logger.error("Error", e);
//            throw Throwables.propagate(e);
            throw ExceptionUtil.rethrow(e);
        }
    }
}
