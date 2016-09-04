package ru.shishmakov.ch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.*;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;
import static ru.shishmakov.hz.DSerializableFactory.*;
import static ru.shishmakov.hz.PSerializableFactory.*;

/**
 * Created by dima on 02.09.16.
 */
public class Chapter9 {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void doExamples(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
//        useSerializition(hz1, hz2);
        useExternalization(hz1, hz2);
//        useDataSerializable(hz1, hz2);
//        useIdentifiedDataSerializable(hz1, hz2);
//        usePortable(hz1, hz2);
    }

    private static void usePortable(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ Portable --");

        processKeyMap(hz1, hz2, new PersonPortable1("Dmitriy", "Alexandrovich", "Shishmakov", "History"));
        processKeyMap(hz1, hz2, new PersonPortable2("Igor", "Alexandrovich", "Shishmakov", "History"));
    }

    private static void useIdentifiedDataSerializable(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ IdentifiedDataSerializable --");

        processKeyMap(hz1, hz2, new PersonIdentDataSerial1("Dmitriy", "Shishmakov", "History"));
        processKeyMap(hz1, hz2, new PersonIdentDataSerial2("Igor", "Shishmakov", "History"));
    }

    private static void useDataSerializable(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ DataSerializable --");

        processKeyMap(hz1, hz2, new PersonDataSerial("Dmitriy", "Shishmakov", "History"));
    }

    private static void useExternalization(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ Externalizable --");

        processKeyMap(hz1, hz2, new PersonExternal("Dmitriy", "Shishmakov", "History"));
    }

    private static void useSerializition(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ Serializable --");

        processKeyMap(hz1, hz2, new PersonSerial("Dmitriy", "Alexandrovich", "Shishmakov", "History"));
    }

    private static void processKeyMap(HazelcastInstance hz1, HazelcastInstance hz2, Person person) {
        IMap<Person, Integer> mapBin = hz1.getMap("mapBin");
        IMap<Person, Integer> mapObj = hz1.getMap("mapObj");

        logger.debug("original key: {}", person);

        mapBin.set(person, 2);
        mapObj.set(person, 2);
        logger.debug("mapBin contains key: {}", mapBin.containsKey(person));
        logger.debug("mapObj contains key: {}", mapObj.containsKey(person));

        Person[] mapBinKeys = mapBin.keySet().toArray(new Person[0]);
        Person[] mapObjKeys = mapObj.keySet().toArray(new Person[0]);
        logger.debug("mapBin keys: {}", Arrays.toString(mapBinKeys));
        logger.debug("mapObj keys: {}", Arrays.toString(mapObjKeys));

        logger.debug("mapBin contains key: {}", mapBin.containsKey(mapBinKeys[0]));
        logger.debug("mapObj contains key: {}", mapObj.containsKey(mapObjKeys[0]));

        mapBin.destroy();
        mapObj.destroy();
    }

    public static class PersonPortable1 extends Person<PersonPortable1> implements Portable {

        private String secondName;

        public PersonPortable1() {
            super();
            /* need to be */
        }

        public PersonPortable1(String name, String secondName, String surname, String hobby) {
            super(name, surname, hobby);
            this.secondName = secondName;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("name", name);
            writer.writeUTF("secondName", secondName);
            logger.debug("-->  write fields name: {}, secondName: {}", name, secondName);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.secondName = reader.readUTF("secondName");
            this.name = reader.readUTF("name");
            logger.debug("<--  read fields name: {}, secondName: {}", name, secondName);
        }

        @Override
        protected void addToString(ToStringBuilder builder) {
            builder.append("secondName", secondName);
        }

        @Override
        protected boolean addToEquals(PersonPortable1 that) {
            return Objects.equals(secondName, that.secondName);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID_200;
        }

        @Override
        public int getClassId() {
            return CLASS_ID_201;
        }
    }

    public static class PersonPortable2 extends Person<PersonPortable2> implements Portable {

        private String secondName;

        public PersonPortable2() {
            super();
            /* need to be */
        }

        public PersonPortable2(String name, String secondName, String surname, String hobby) {
            super(name, surname, hobby);
            this.secondName = secondName;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("secondName", secondName);
            writer.writeUTF("name", name);
            logger.debug("-->  write fields name: {}, secondName: {}", name, secondName);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.name = reader.readUTF("name");
            this.secondName = reader.readUTF("secondName");
            logger.debug("<--  read fields name: {}, secondName: {}", name, secondName);
        }

        @Override
        protected void addToString(ToStringBuilder builder) {
            builder.append("secondName", secondName);
        }

        @Override
        protected boolean addToEquals(PersonPortable2 that) {
            return Objects.equals(secondName, that.secondName);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID_200;
        }

        @Override
        public int getClassId() {
            return CLASS_ID_202;
        }
    }

    public static class PersonIdentDataSerial1 extends Person<PersonIdentDataSerial1> implements IdentifiedDataSerializable {

        public PersonIdentDataSerial1() {
            super();
            /* need to be */
        }

        public PersonIdentDataSerial1(String name, String surname, String hobby) {
            super(name, surname, hobby);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
            logger.debug("-->  write field name: {}", name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.name = in.readUTF();
            logger.debug("<--  read field name: {}", name);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID_100;
        }

        @Override
        public int getId() {
            return CLASS_ID_101;
        }
    }

    public static class PersonIdentDataSerial2 extends Person<PersonIdentDataSerial2> implements IdentifiedDataSerializable {

        public PersonIdentDataSerial2() {
            super();
            /* need to be */
        }

        public PersonIdentDataSerial2(String name, String surname, String hobby) {
            super(name, surname, hobby);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
            logger.debug("-->  write field name: {}", name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.name = in.readUTF();
            logger.debug("<--  read field name: {}", name);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID_100;
        }

        @Override
        public int getId() {
            return CLASS_ID_102;
        }
    }


    public static class PersonDataSerial extends Person<PersonDataSerial> implements DataSerializable {

        public PersonDataSerial() {
            super();
            /* need to be */
        }

        public PersonDataSerial(String name, String surname, String hobby) {
            super(name, surname, hobby);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
            logger.debug("-->  write field name: {}", name);

        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.name = in.readUTF();
            logger.debug("<--  read field name: {}", name);
        }
    }


    public static class PersonExternal extends Person<PersonExternal> implements Externalizable {

        public PersonExternal() {
            super();
            /* need to be */
        }

        public PersonExternal(String name, String surname, String hobby) {
            super(name, surname, hobby);
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(name);
            logger.debug("-->  write field name: {}", name);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.name = in.readUTF();
            logger.debug("<--  read field name: {}", name);
        }
    }

    public static class PersonSerial extends Person<PersonSerial> implements Serializable {
        private static final long serialVersionUID = 1L;

        private String secondName; // only this field will be deserialized

        public PersonSerial(String name, String secondName, String surname, String hobby) {
            super(name, surname, hobby);
            this.secondName = secondName;
        }

        @Override
        protected void addToString(ToStringBuilder builder) {
            builder.append("secondName", secondName);
        }

        @Override
        protected boolean addToEquals(PersonSerial that) {
            return Objects.equals(secondName, that.secondName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), secondName);
        }
    }


    public abstract static class Person<T extends Person<T>> {
        protected /* not final */ String name;
        protected /* not final */ transient String surname;
        protected /* not final */ transient String hobby = "rugby";
        protected static /* not final */ String password = "password123";

        Person() {
            /* need to be */
        }

        protected Person(String name, String surname, String hobby) {
            this.name = name;
            this.surname = surname;
            this.hobby = hobby;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof Person)) return false;
            T that = (T) o;
            return Objects.equals(name, that.name) &&
                    addToEquals(that) &&
                    Objects.equals(surname, that.surname) &&
                    Objects.equals(hobby, that.hobby);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, surname, hobby);
        }

        @Override
        public String toString() {
            ToStringBuilder builder = new ToStringBuilder(this, SHORT_PREFIX_STYLE);
            builder.append("name", this.name).append("surname", surname);
            addToString(builder);
            return builder
                    .append("hobby", hobby)
                    .append("password", password)
                    .toString();
        }

        protected boolean addToEquals(T that) {
            /* for additional fields */
            return true;
        }

        protected void addToString(ToStringBuilder builder) {
            /* for additional fields */
        }

        public String getName() {
            return name;
        }

        public String getSurname() {
            return surname;
        }

        public String getHobby() {
            return hobby;
        }

        public static String getPassword() {
            return password;
        }
    }
}
