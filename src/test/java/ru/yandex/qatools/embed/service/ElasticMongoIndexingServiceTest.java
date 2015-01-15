package ru.yandex.qatools.embed.service;

import org.javalite.common.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.qatools.embed.service.beans.IndexingResult;
import ru.yandex.qatools.embed.service.db.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static ch.lambdaj.Lambda.collect;
import static ch.lambdaj.Lambda.on;
import static com.mongodb.ReadPreference.nearest;
import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static java.lang.Thread.sleep;
import static org.hamcrest.Matchers.containsInAnyOrder;
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

        es.updateMappings(Collections.<String, Map<String, Object>>map(
                "posts._id", Collections.<String, Object>map(
                        "type", "string", "index", "not_analyzed", "store", true),
                "posts.user.detail", Collections.<String, Object>map(
                        "type", "object", "enabled", false),
                "users.detail", Collections.<String, Object>map(
                        "type", "object", "enabled", false)
        ));

//        es.updateIndexSettings(Collections.<String, Object>map(
//                "index.mapping.ignore_malformed", true,
//                "index.fail_on_merge_failure", false
//        ));

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
        UserMongo user1 = createUser(1L, "Ivan Petrov", new UserDetailMongoLong(1L));
        sleep(1000);
        UserMongo user2 = createUser(2L, "Ivan Ivanov", new UserDetailMongoString("some-string"));
        sleep(1000);
        PostMongo post1 = createPost(user1, "Some title", "Some post with keyword among other words");
        sleep(1000);
        PostMongo post2 = createPost(user2, "Some another title", "Some post without the required word");
        sleep(1000);
        PostMongo post3 = createPost(user2, "Some third title", "Some post with the required keyword among other words");

        assertThat("At least two users must be found by query",
                es, should(findIndexedAtLeast(UserMongo.class, "users", "name:Ivan", 2))
                        .whileWaitingUntil(timeoutHasExpired(20000)));
        final List<IndexingResult> users = es.search("users", "name:(Ivan AND Petrov)");
        assertThat(users, hasSize(1));
        assertThat(users.get(0).getId(), is(String.valueOf(user1.getId())));

        assertThat("Post must be found by id",
                es, should(findIndexedAtLeast(PostMongo.class, "posts", "_id:\"" + post1.getId() + "\"", 1))
                        .whileWaitingUntil(timeoutHasExpired(20000)));

        assertThat("At least two posts must be found by query",
                es, should(findIndexedAtLeast(PostMongo.class, "posts", "body:keyword", 2))
                        .whileWaitingUntil(timeoutHasExpired(20000)));

        // perform the search
        final List<IndexingResult> posts = es.search("posts", "body:keyword");
        assertThat(posts, hasSize(2));
        Set<String> postIds = new HashSet<>(collect(posts, on(IndexingResult.class).getId()));
        assertThat(postIds, containsInAnyOrder(post1.getId().toString(), post3.getId().toString()));
    }


    private UserMongo createUser(Long id, String name, UserDetailMongo detail) throws UnknownHostException {
        final UserMongo user = new UserMongo();
        user.setId(id);
        user.setName(name);
        user.setDetail(detail);
        userDAO.save(user);
        return user;
    }

    private PostMongo createPost(UserMongo user, String title, String description) throws UnknownHostException {
        final PostMongo post = new PostMongo();
        post.setTitle(title);
        post.setUser(user);
        post.setBody(description);
        postDAO.save(post);
        return post;
    }

}