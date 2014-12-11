package ru.yandex.qatools.embed.service.db;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.MongoCredential.createMongoCRCredential;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author smecsia
 */
public class MorphiaDBService {

    private static final String HOST_PORT_SPLIT_PATTERN = "(?<!:):(?=[123456789]\\d*$)";
    private final Datastore datastore;
    private final MongoClient mongoClient;

    public MorphiaDBService(String replicaSet, String dbName, String username, String password)
            throws UnknownHostException {
        List<ServerAddress> addresses = new ArrayList<>();
        for (String host : replicaSet.split(",")) {
            String[] hostPort = host.split(HOST_PORT_SPLIT_PATTERN);
            addresses.add(new ServerAddress(hostPort[0], Integer.valueOf(hostPort[1])));
        }
        mongoClient = ((!isEmpty(username) && !isEmpty(password))) ?
                new MongoClient(addresses, asList(createMongoCRCredential(username, dbName, password.toCharArray()))) :
                new MongoClient(addresses);
        datastore = new Morphia().createDatastore(mongoClient, dbName);
    }

    public Datastore getDatastore() {
        return datastore;
    }
}
