package ru.yandex.qatools.embed.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.qatools.embed.service.beans.IndexingResult;
import ru.yandex.qatools.embed.service.db.*;

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
    public static final int INIT_TIMEOUT = 10000;
    ElasticMongoIndexingService es;
    MongoEmbeddedService mongo;
    PostDAO postDAO;
    UserDAO userDAO;

    @Before
    public void startEmbeddedServers() throws IOException, InterruptedException {
        mongo = new MongoEmbeddedService(RS, DB, USER, PASS, RS_NAME, null, true, INIT_TIMEOUT);
        mongo.start();
        es = new ElasticMongoIndexingService(RS, DB, USER, PASS, null, true, INIT_TIMEOUT);
        es.start();

        final MorphiaDBService dbService = new MorphiaDBService(RS, DB, USER, PASS);
        dbService.getDatastore().getDB().getMongo().setReadPreference(nearest());
        dbService.getDatastore().setDefaultWriteConcern(ACKNOWLEDGED);
        postDAO = new PostDAO(dbService);
        userDAO = new UserDAO(dbService);

        es.indexAllCollections();
    }

    @After
    public void shutdownEmbeddedServers() throws IOException {
        mongo.stop();
        es.stop();
    }

    @Test
    public void testElasticFullTextSearch() throws IOException, InterruptedException {
        // create some test data
        UserMongo user1 = createUser("Ivan Petrov");
        UserMongo user2 = createUser("Ivan Ivanov");
        PostMongo post1 = createPost(user1, "Some title", "Some post with keyword among other words");
        PostMongo post2 = createPost(user2, "Some another title", "Some post without the required word");
        PostMongo post3 = createPost(user2, "Some third title", "Some post with the required keyword among other words");

        assertThat("At least two users must be found by query",
                es, should(findIndexedAtLeast(UserMongo.class, "users", "name:Ivan", 2))
                        .whileWaitingUntil(timeoutHasExpired(20000)));
        final List<IndexingResult> users = es.search("users", "name:(Ivan AND Petrov)");
        assertThat(users, hasSize(1));
        assertThat(users.get(0).getId(), is(user1.getId().toString()));

        assertThat("At least two posts must be found by query",
                es, should(findIndexedAtLeast(PostMongo.class, "posts", "body:keyword", 2))
                        .whileWaitingUntil(timeoutHasExpired(20000)));

        // perform the search
        final List<IndexingResult> posts = es.search("posts", "body:keyword");
        assertThat(posts, hasSize(2));
        assertThat(posts.get(0).getId(), is(post1.getId().toString()));
        assertThat(posts.get(1).getId(), is(post3.getId().toString()));
    }


    private UserMongo createUser(String name) throws UnknownHostException {
        final UserMongo user = new UserMongo();
        user.setName(name);
        userDAO.save(user);
        return user;
    }

    private PostMongo createPost(UserMongo user, String title, String description) throws UnknownHostException {
        final PostMongo post = new PostMongo();
        post.setTitle(title);
        post.setUserId(user.getId());
        post.setBody(description);
        postDAO.save(post);
        return post;
    }

}