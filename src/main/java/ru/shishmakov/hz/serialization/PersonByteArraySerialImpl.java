package ru.shishmakov.hz.serialization;

import com.hazelcast.nio.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter9_Serialization.PersonByteArraySerial;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;

import static ru.shishmakov.hz.serialization.SerializerIds.PERSON_BYTE_ARRAY_SERIALIZER;

/**
 * @author Dmitriy Shishmakov
 */
public class PersonByteArraySerialImpl implements ByteArraySerializer<PersonByteArraySerial> {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public byte[] write(PersonByteArraySerial person) throws IOException {
        byte[] bytes = person.getName().getBytes(StandardCharsets.UTF_8);
        logger.debug("-->  write person: {}", person);
        return bytes;
    }

    @Override
    public PersonByteArraySerial read(byte[] buffer) throws IOException {
        PersonByteArraySerial person = new PersonByteArraySerial();
        person.setName(new String(buffer, StandardCharsets.UTF_8));
        logger.debug("<--  read person: {}", person);
        return person;
    }

    @Override
    public int getTypeId() {
        return PERSON_BYTE_ARRAY_SERIALIZER;
    }

    @Override
    public void destroy() {
        logger.debug("x--  destroy serializer: {}", this);
    }
}
