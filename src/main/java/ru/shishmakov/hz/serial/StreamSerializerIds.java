package ru.shishmakov.hz.serial;

/**
 * Created by dima on 10.09.16.
 */
public final class StreamSerializerIds {
    private static int counter = 300;

    public static final int PERSON_SERIALIZER = counter++;
    public static final int PERSON_SERIALIZER_2 = counter++;
    public static final int PERSON_SERIALIZER_3 = counter++;
    public static final int PERSON_KRYO_SERIALIZER = counter++;
    public static final int SMART_KRYO_SERIALIZER = counter++;
    public static final int MAP_SERIALIZER = counter++;
}
