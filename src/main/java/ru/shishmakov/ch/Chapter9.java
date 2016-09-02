package ru.shishmakov.ch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Created by dima on 02.09.16.
 */
public class Chapter9 {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void doExamples(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        serializable(hz1, hz2);
    }

    private static void serializable(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ serializable --");

        IMap<PersonSerial, Integer> mapBin = hz1.getMap("mapBin");
        IMap<PersonSerial, Integer> mapObj = hz1.getMap("mapObj");

        PersonSerial person = new PersonSerial("Dima", "Shishmakov", "History");
        logger.debug("original key: {}", person);

        mapBin.set(person, 1);
        mapObj.set(person, 1);
        logger.debug("mapBin contains key: {}", mapBin.containsKey(person));
        logger.debug("mapObj contains key: {}", mapObj.containsKey(person));

        PersonSerial[] mapBinKeys = mapBin.keySet().toArray(new PersonSerial[0]);
        PersonSerial[] mapObjKeys = mapObj.keySet().toArray(new PersonSerial[0]);
        logger.debug("mapBin keys: {}", Arrays.toString(mapBinKeys));
        logger.debug("mapObj keys: {}", Arrays.toString(mapObjKeys));

        logger.debug("mapBin contains key: {}", mapBin.containsKey(mapBinKeys[0]));
        logger.debug("mapObj contains key: {}", mapObj.containsKey(mapObjKeys[0]));

        mapBin.destroy();
        mapObj.destroy();
    }

    public static class PersonSerial implements Serializable {
        private static final long serialVersionUID = 1L;

        private /* not final */ String name;
        private /* not final */ transient String surname;
        private /* not final */ transient String hobby = "rugby";
        private static /* not final */ String password = "password123";

        public PersonSerial(String name, String surname, String hobby) {
            this.name = name;
            this.surname = surname;
            this.hobby = hobby;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof PersonSerial)) return false;
            PersonSerial that = (PersonSerial) o;
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
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .append("name", name)
                    .append("surname", surname)
                    .append("hobby", hobby)
                    .append("password", password)
                    .toString();
        }
    }
}
