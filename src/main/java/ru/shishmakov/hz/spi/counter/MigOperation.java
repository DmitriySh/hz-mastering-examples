package ru.shishmakov.hz.spi.counter;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * During the execution of a migration, no other operations will be running in that partition.
 * Therefore, you don’t need to deal with thread-safety.
 *
 * @author Dmitriy Shishmakov on 24.10.16
 */
public class MigOperation extends AbstractOperation {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String CLASS_NAME = MigOperation.class.getSimpleName();

    private Map<String, Integer> migrationData = Collections.emptyMap();

    {
        logger.debug("Create instance {}", CLASS_NAME);
    }

    public MigOperation() {
        /* need to be */
    }

    public MigOperation(Map<String, Integer> data) {
        this.migrationData = data;
        logger.debug("{}, migration data: {}", this.getClass().getSimpleName(), data);
    }

    @Override
    public void run() throws Exception {
        CounterService service = getService();
        CounterContainer container = service.getContainerByPartitionId(getPartitionId());
        container.applyMigrationData(migrationData);
        logger.debug("Apply migration data: {}, address: {}", migrationData, getNodeEngine().getThisAddress());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        migrationData.forEach((k, v) -> {
            try {
                out.writeUTF(k);
                out.writeInt(v);
            } catch (IOException e) {
                logger.error("Error", e);
            }
        });
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        Map<String, Integer> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            map.put(in.readUTF(), in.readInt());
        }
        migrationData = map;

    }
}
