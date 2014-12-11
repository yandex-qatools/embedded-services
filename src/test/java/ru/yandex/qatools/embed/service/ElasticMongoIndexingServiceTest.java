package ru.yandex.qatools.embed.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.qatools.embed.service.beans.IndexingResult;
import ru.yandex.qatools.embed.service.db.MorphiaDBService;
import ru.yandex.qatools.embed.service.db.PostMongo;
import ru.yandex.qatools.embed.service.db.PostDAO;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import static com.mongodb.ReadPreference.nearest;
import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static ru.yandex.qatools.embed.service.IndexingServiceMatcher.findIndexedAtLeast;
import static ru.yandex.qatools.matchers.decorators.MatcherDecorators.should;
import static ru.yandex.qatools.matchers.decorators.MatcherDecorators.timeoutHasExpired;

public class ElasticMongoIndexingServiceTest {
    public static final String RS_NAME = "local";
    public static final String RS = "localhost:37017";
    public static final String DB = "mongolastic";
    public static final String USER = "user";
    public static final String PASS = "pass";
    ElasticMongoIndexingService es;
    MongoEmbeddedService mongo;
    PostDAO postDAO;

    @Before
    public void startEmbeddedServers() throws IOException, InterruptedException {
        mongo = new MongoEmbeddedService(RS, DB, USER, PASS, RS_NAME, null, true);
        mongo.start();
        es = new ElasticMongoIndexingService(RS, DB, USER, PASS, null, true, 25000);
        es.start();

        final MorphiaDBService dbService = new MorphiaDBService(RS, DB, USER, PASS);
        dbService.getDatastore().getDB().getMongo().setReadPreference(nearest());
        dbService.getDatastore().setDefaultWriteConcern(ACKNOWLEDGED);
        postDAO = new PostDAO(dbService);
    }

    @After
    public void shutdownEmbeddedServers() throws IOException {
        mongo.stop();
        es.stop();
    }

    @Test
    public void testElasticFullTextSearch() throws IOException, InterruptedException {
        // create some test data
        PostMongo post1 = createPost("Some title", "Some post with keyword among other words");
        PostMongo post2 = createPost("Some another title", "Some post without the required word");
        PostMongo post3 = createPost("Some third title", "Some post with the required keyword among other words");

        es.addToIndex("post"); // create the new index within elastic using the mongodb river

        assertThat("At least two posts must be found by query",
                es, should(findIndexedAtLeast(PostMongo.class, "post", "body:keyword", 2))
                       .whileWaitingUntil(timeoutHasExpired(20000)));

        // perform the search
        final List<IndexingResult> response = es.search("post", "body:keyword");
        assertThat(response, hasSize(2));
        assertThat(response.get(0).getId(), is(post1.getId().toString()));
        assertThat(response.get(1).getId(), is(post3.getId().toString()));
    }


    private PostMongo createPost(String title, String description) throws UnknownHostException {
        final PostMongo post = new PostMongo();
        post.setTitle(title);
        post.setBody(description);
        postDAO.save(post);
        return post;
    }

}