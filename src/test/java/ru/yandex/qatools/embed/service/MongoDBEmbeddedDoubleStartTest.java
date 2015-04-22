package ru.yandex.qatools.embed.service;

import de.flapdoodle.embed.mongo.distribution.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.qatools.embed.service.db.*;

import java.io.IOException;
import java.net.UnknownHostException;

import static ch.lambdaj.Lambda.collect;
import static ch.lambdaj.Lambda.on;
import static com.mongodb.ReadPreference.nearest;
import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static jodd.io.FileUtil.createTempDirectory;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class MongoDBEmbeddedDoubleStartTest {
    public static final String RS = "localhost:37017";
    public static final String DB = "mongolastic";
    public static final String RS_NAME = "local";
    public static final String USER = "user";
    public static final String PASS = "password";
    private String tmpDir;
    MongoEmbeddedService mongo;
    UserDAO userDAO;

    @Before
    public void startEmbeddedServers() throws IOException, InterruptedException {
        tmpDir = createTempDirectory(getClass().getSimpleName().toLowerCase(), "data").getPath();
        mongo = new MongoEmbeddedService(RS, DB, USER, PASS, RS_NAME, tmpDir, true, 10000)
                .useVersion(Version.Main.PRODUCTION).useWiredTiger();
        mongo.start();
        mongo.stop();
        mongo.start();
        final MorphiaDBService dbService = new MorphiaDBService(RS, DB, USER, PASS);
        dbService.getDatastore().getDB().getMongo().setReadPreference(nearest());
        dbService.getDatastore().setDefaultWriteConcern(ACKNOWLEDGED);
        userDAO = new UserDAO(dbService);
    }

    @After
    public void shutdownEmbeddedServers() throws IOException {
        mongo.stop();
    }

    @Test
    public void testElasticFullTextSearch() throws IOException, InterruptedException {
        // create some test data
        UserMongo user1 = createUser(1L, "Ivan Petrov", new UserDetailMongoLong(1L));
        assertThat(collect(userDAO.find().asList(), on(UserMongo.class).getId()), contains(user1.getId()));
    }


    private UserMongo createUser(Long id, String name, UserDetailMongo detail) throws UnknownHostException {
        final UserMongo user = new UserMongo();
        user.setId(id);
        user.setName(name);
        user.setDetail(detail);
        userDAO.save(user);
        return user;
    }
}