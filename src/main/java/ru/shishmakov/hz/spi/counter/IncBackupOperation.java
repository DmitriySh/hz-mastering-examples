package ru.shishmakov.hz.spi.counter;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * {@link IncOperation} for specific particular key will not be executing before
 * the (synchronous) backup operation has completed
 *
 * @author Dmitriy Shishmakov on 30.10.16
 */
public class IncBackupOperation extends AbstractOperation implements BackupOperation {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String CLASS_NAME = IncBackupOperation.class.getSimpleName();

    private String objectId;
    private int delta;

    {
        logger.debug("Create instance {}", CLASS_NAME);
    }

    public IncBackupOperation() {
        /* need to be */
    }

    public IncBackupOperation(String objectId, int delta) {
        this.objectId = objectId;
        this.delta = delta;
    }

    @Override
    public void run() throws Exception {
        CounterService service = getService();
        CounterContainer container = service.getContainerByPartitionId(getPartitionId());
        container.increment(objectId, delta);
        logger.debug("Executing backup {}.increment() on: {}", objectId, getNodeEngine().getThisAddress());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(objectId);
        out.writeInt(delta);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.objectId = in.readUTF();
        this.delta = in.readInt();
    }
}
