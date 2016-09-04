package ru.shishmakov.ch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

/**
 * Created by dima on 02.09.16.
 */
public class Chapter9 {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void doExamples(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
//        useSerializition(hz1, hz2);
//        useExternalization(hz1, hz2);
        useDataSerializable(hz1, hz2);
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

    public static class PersonDataSerial extends Person implements DataSerializable {

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

        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.name = in.readUTF();

        }
    }


    public static class PersonExternal extends Person implements Externalizable {

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
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.name = in.readUTF();
        }
    }

    public static class PersonSerial extends Person implements Serializable {
        private static final long serialVersionUID = 1L;

        private String secondName; // only this field will be deserialized

        public PersonSerial(String name, String secondName, String surname, String hobby) {
            super(name, surname, hobby);
            this.secondName = secondName;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                    .append("name", name)
                    .append("secondName", secondName)
                    .append("surname", surname)
                    .append("hobby", hobby)
                    .append("password", password)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof PersonSerial)) return false;
            if (!super.equals(o)) return false;
            PersonSerial that = (PersonSerial) o;
            return Objects.equals(secondName, that.secondName) &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(surname, that.surname) &&
                    Objects.equals(hobby, that.hobby);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), secondName);
        }
    }


    public static class Person {
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
            Person that = (Person) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(surname, that.surname) &&
                    Objects.equals(hobby, that.hobby);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, surname, hobby);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                    .append("name", name)
                    .append("surname", surname)
                    .append("hobby", hobby)
                    .append("password", password)
                    .toString();
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
