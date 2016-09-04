package ru.shishmakov.hz;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import ru.shishmakov.ch.Chapter9;

/**
 * Created by dima on 04.09.16.
 */
public class DSerializableFactory implements DataSerializableFactory {

    public static final int FACTORY_ID_100 = 100;
    public static final int CLASS_ID_101 = 101;
    public static final int CLASS_ID_102 = 102;

    @Override
    public IdentifiedDataSerializable create(int classId) {
        if (classId == CLASS_ID_101) {
            return new Chapter9.PersonIdentDataSerial1();
        }
        if (classId == CLASS_ID_102) {
            return new Chapter9.PersonIdentDataSerial2();
        }
        throw new IllegalStateException("Class id: " + classId + "not found");
    }
}
