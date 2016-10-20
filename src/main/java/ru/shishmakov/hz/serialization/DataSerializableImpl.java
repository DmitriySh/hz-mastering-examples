package ru.shishmakov.hz.serialization;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import ru.shishmakov.ch.Chapter9_Serialization;

import static ru.shishmakov.hz.serialization.SerializerIds.DATA_SERIAL_ID_1;
import static ru.shishmakov.hz.serialization.SerializerIds.DATA_SERIAL_ID_2;

/**
 * @author Dmitriy Shishmakov
 */
public class DataSerializableImpl implements DataSerializableFactory {

    @Override
    public IdentifiedDataSerializable create(int classId) {
        if (classId == DATA_SERIAL_ID_1) {
            return new Chapter9_Serialization.PersonIdentDataSerial1();
        }
        if (classId == DATA_SERIAL_ID_2) {
            return new Chapter9_Serialization.PersonIdentDataSerial2();
        }
        throw new IllegalStateException("Class id: " + classId + "not found");
    }
}
