package ru.shishmakov.hz.serial;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import ru.shishmakov.ch.Chapter9;

import static ru.shishmakov.hz.serial.SerializerIds.DATA_SERIAL_ID_1;
import static ru.shishmakov.hz.serial.SerializerIds.DATA_SERIAL_ID_2;

/**
 * Created by dima on 04.09.16.
 */
public class DataSerializableImpl implements DataSerializableFactory {

    @Override
    public IdentifiedDataSerializable create(int classId) {
        if (classId == DATA_SERIAL_ID_1) {
            return new Chapter9.PersonIdentDataSerial1();
        }
        if (classId == DATA_SERIAL_ID_2) {
            return new Chapter9.PersonIdentDataSerial2();
        }
        throw new IllegalStateException("Class id: " + classId + "not found");
    }
}
