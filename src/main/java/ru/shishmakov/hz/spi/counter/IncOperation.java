package ru.shishmakov.hz.spi.counter;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * Should be executed on the partition hosting of 'counter'.
 *
 * @author Dmitriy Shishmakov on 19.10.16
 */
class IncOperation extends AbstractOperation implements PartitionAwareOperation, BackupAwareOperation {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String CLASS_NAME = IncOperation.class.getSimpleName();

    private String objectId;
    private int delta;
    private int value;

    {
        logger.debug("Create instance {}", CLASS_NAME);
    }

    public IncOperation() {
        /* need to be */
    }

    IncOperation(String objectId, int delta) {
        this.objectId = objectId;
        this.delta = delta;
    }

    /**
     * Responsible for the actual execution
     */
    @Override
    public void run() throws Exception {
        final int partitionId = getPartitionId();
        CounterService service = getService();
        CounterContainer container = service.getContainerByPartitionId(partitionId);
        container.increment(objectId, delta);
        value = container.getCount(objectId);
        logger.debug("Execute {}.increment value: {} partition: {} on: {}",
                objectId, value, partitionId, getNodeEngine().getThisAddress());
    }

    /**
     * Each inc operation is going to return a response.
     *
     * @return true - synchronous mode notifies about availability new value;<br/>
     * false - asynchronous mode notifies about not need to return a response
     * (it is better to return false because that is faster)
     */
    @Override
    public boolean returnsResponse() {
        return true;
    }

    /**
     * @return actual response value
     */
    @Override
    public Object getResponse() {
        return value;
    }

    /**
     * Needs to be serialized
     */
    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(objectId);
        out.writeInt(delta);
    }

    /**
     * Needs to be deserialized
     */
    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        objectId = in.readUTF();
        delta = in.readInt();
    }

    /**
     * @return true is the operation needs a backup; false is otherwise
     */
    @Override
    public boolean shouldBackup() {
        return true;
    }

    /**
     * @return count of synchronous backups
     */
    @Override
    public int getSyncBackupCount() {
        return 1;
    }

    /**
     * @return count of asynchronous backups
     */
    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    /**
     * @return operation that is going to make the backup
     */
    @Override
    public Operation getBackupOperation() {
        return new IncBackupOperation(objectId, delta);
    }
}
