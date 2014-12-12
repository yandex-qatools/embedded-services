package ru.yandex.qatools.embed.service;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticMongoIndexingService extends AbstractElasticEmbeddedService {

    private final String mongoReplicaSet;
    private final String mongoDBName;
    private final String username;
    private final String password;
    private final boolean enabled;

    public ElasticMongoIndexingService(String mongoReplicaSet, String mongoDatabaseName,
                                        String mongoUsername, String mongoPassword) throws IOException {
        this(mongoReplicaSet, mongoDatabaseName, mongoUsername, mongoPassword, null, true, 10000);
    }

    public ElasticMongoIndexingService(
            String mongoReplicaSet,
            String mongoDatabaseName,
            String mongoUsername,
            String mongoPassword,
            String dataDirectory,
            boolean enabled,
            int initTimeout
    ) throws IOException {
        super(dataDirectory, enabled, initTimeout);
        this.mongoReplicaSet = mongoReplicaSet;
        this.mongoDBName = mongoDatabaseName;
        this.enabled = enabled;
        this.username = mongoUsername;
        this.password = mongoPassword;
    }

    @Override
    protected synchronized void indexCollection(String collectionName) throws IOException {
        if (enabled) {
            final XContentBuilder config =
                    jsonBuilder()
                    .startObject()
                        .field("type", "mongodb")
                        .startObject("mongodb");
                            config
                            .startArray("servers");
                                for (String replSetEl : mongoReplicaSet.split(",")) {
                                    final String[] hostPort = replSetEl.split(":");
                                    config
                                        .startObject()
                                            .field("host", hostPort[0])
                                            .field("port", Integer.parseInt(hostPort[1]))
                                        .endObject();
                                }
                                config
                            .endArray();
                                config
                            .startArray("credentials")
                                .startObject()
                                    .field("db", "local")
                                    .field("auth", mongoDBName)
                                    .field("user", username)
                                    .field("password", password)
                                .endObject()
                                .startObject()
                                    .field("db", mongoDBName)
                                    .field("auth", mongoDBName)
                                    .field("user", username)
                                    .field("password", password)
                                .endObject()
                            .endArray();
                            config
                            .field("db", mongoDBName)
                            .field("collection", collectionName)
                            .field("gridfs", false)
                            .startObject("options")
                                .field("secondary_read_preference", "false")
                                .field("drop_collection", "true")
                                .field("is_mongos", "false")
                            .endObject()
                        .endObject()
                        .startObject("index")
                            .field("name", mongoDBName)
                            .field("type", collectionName)
                            .field("bulk_size", "1000")
                            .field("bulk_timeout", "30")
                        .endObject()
                    .endObject();
            getClient().prepareIndex("_river", collectionName, "_meta").setSource(config)
                    .execute().actionGet(initTimeout);
            logger.info("Collection {}.{} indexing request sent to ES", mongoDBName, collectionName);
        }
    }
}
