package ru.shishmakov.hz.serial;

/**
 * Created by dima on 10.09.16.
 */
public final class SerializerIds {
    private static int counter = 300;

    public static final int DATA_SERIAL_FACTORY_ID = 100;
    public static final int DATA_SERIAL_ID_1 = counter++;
    public static final int DATA_SERIAL_ID_2 = counter++;

    public static final int PERSON_SERIALIZER = counter++;
    public static final int PERSON_SERIALIZER_2 = counter++;
    public static final int PERSON_SERIALIZER_3 = counter++;

    public static final int PERSON_KRYO_SERIALIZER = counter++;
    public static final int SMART_KRYO_SERIALIZER = counter++;

    public static final int PERSON_BYTE_ARRAY_SERIALIZER = counter++;

    public static final int MAP_SERIALIZER = counter++;
}
