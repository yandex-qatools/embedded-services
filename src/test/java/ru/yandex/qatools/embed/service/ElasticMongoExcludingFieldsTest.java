package ru.yandex.qatools.embed.service;

import de.flapdoodle.embed.mongo.distribution.Version;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import ru.yandex.qatools.embed.service.db.MorphiaDBService;
import ru.yandex.qatools.embed.service.db.PostDAO;
import ru.yandex.qatools.embed.service.db.UserDAO;

import java.io.IOException;

import static com.mongodb.ReadPreference.nearest;
import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static ru.yandex.qatools.embed.service.util.SocketUtil.findFreePort;

public class ElasticMongoExcludingFieldsTest extends ElasticMongoIndexingServiceTest {

    @Before
    @Override
    public void startEmbeddedServers() throws IOException, InterruptedException {
        final String RS = "localhost:" + findFreePort();
        mongo = new MongoEmbeddedService(RS, DB, USER, PASS, RS_NAME, null, true, INIT_TIMEOUT)
                .useVersion(Version.Main.PRODUCTION).useWiredTiger();
        mongo.start();
        final MorphiaDBService dbService = new MorphiaDBService(RS, DB, USER, PASS);
        dbService.getDatastore().getDB().getMongo().setReadPreference(nearest());
        dbService.getDatastore().setDefaultWriteConcern(ACKNOWLEDGED);
        postDAO = new PostDAO(dbService);
        userDAO = new UserDAO(dbService);

        es = new ElasticMongoIndexingService(RS, DB, USER, PASS, null, true, INIT_TIMEOUT) {
            @Override
            protected void addIndexAllCollectionsOptions(XContentBuilder config) {
                try {
                    config.array("exclude_fields", "detail", "user.detail");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        es.setBindHost("localhost");
        es.start();
        es.indexAll();
    }
}