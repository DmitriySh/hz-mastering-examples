package ru.shishmakov.hz.serial;

import com.hazelcast.nio.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter9.PersonByteArraySerial2;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;

import static ru.shishmakov.hz.serial.SerializerIds.PERSON_BYTE_ARRAY_SERIALIZER_2;

/**
 * @author Dmitriy Shishmakov
 */
public class PersonByteArraySerial2Impl implements ByteArraySerializer<PersonByteArraySerial2> {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public byte[] write(PersonByteArraySerial2 person) throws IOException {
        byte[] bytes = person.getName().getBytes(StandardCharsets.UTF_8);
        logger.debug("-->  write person: {}", person);
        return bytes;
    }

    @Override
    public PersonByteArraySerial2 read(byte[] buffer) throws IOException {
        PersonByteArraySerial2 person = new PersonByteArraySerial2();
        person.setName(new String(buffer, StandardCharsets.UTF_8));
        logger.debug("<--  read person: {}", person);
        return person;
    }

    @Override
    public int getTypeId() {
        return PERSON_BYTE_ARRAY_SERIALIZER_2;
    }

    @Override
    public void destroy() {
        logger.debug("x--  destroy serializer: {}", this);
    }
}
