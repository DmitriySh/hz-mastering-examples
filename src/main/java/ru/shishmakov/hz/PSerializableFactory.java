package ru.shishmakov.hz;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import ru.shishmakov.ch.Chapter9;

/**
 * Created by dima on 04.09.16.
 */
public class PSerializableFactory implements PortableFactory {

    public static final int FACTORY_ID_200 = 200;
    public static final int CLASS_ID_201 = 201;
    public static final int CLASS_ID_202 = 202;
    public static final int CLASS_ID_203 = 203;

    @Override
    public Portable create(int classId) {
        switch (classId) {
            case CLASS_ID_201:
                return new Chapter9.PersonPortable1();
            case CLASS_ID_202:
                return new Chapter9.PersonPortable2();
            case CLASS_ID_203:
                return new Chapter9.PersonPortable3();
            default:
                throw new IllegalStateException("Class id: " + classId + "not found");
        }
    }
}
