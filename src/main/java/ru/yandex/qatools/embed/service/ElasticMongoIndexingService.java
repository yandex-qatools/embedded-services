package ru.yandex.qatools.embed.service;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticMongoIndexingService extends AbstractElasticEmbeddedService {

    private final String mongoReplicaSet;
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
        super(mongoDatabaseName, dataDirectory, enabled, initTimeout);
        this.mongoReplicaSet = mongoReplicaSet;
        this.enabled = enabled;
        this.username = mongoUsername;
        this.password = mongoPassword;
    }


    protected void addIndexAllCollectionsOptions(XContentBuilder config) {
    }

    @Override
    protected void indexAllCollections() throws IOException {
        if (enabled) {
            final XContentBuilder config =jsonBuilder()
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
                            .field("auth", dbName)
                            .field("user", username)
                            .field("password", password)
                        .endObject()
                        .startObject()
                            .field("db", dbName)
                            .field("auth", dbName)
                            .field("user", username)
                            .field("password", password)
                        .endObject()
                    .endArray();
                        config
                    .field("db", dbName)
                    .field("gridfs", false)
                    .startObject("options")
                        .field("secondary_read_preference", false)
                        .field("drop_collection", true)
                        .field("is_mongos", false)
                        .field("import_all_collections", true);
                        addIndexAllCollectionsOptions(config);
                        config
                    .endObject()
                .endObject()
                .startObject("index")
                    .field("name", dbName)
                    .field("bulk_size", "1000")
                    .field("bulk_timeout", "30")
                .endObject()
            .endObject();
            getClient().prepareIndex("_river", dbName, "_meta").setSource(config)
                    .execute().actionGet(initTimeout);
            logger.info("Collection {}.* indexing request sent to ES", dbName);
        }

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
                                    .field("auth", dbName)
                                    .field("user", username)
                                    .field("password", password)
                                .endObject()
                                .startObject()
                                    .field("db", dbName)
                                    .field("auth", dbName)
                                    .field("user", username)
                                    .field("password", password)
                                .endObject()
                            .endArray();
                            config
                            .field("db", dbName)
                            .field("collection", collectionName)
                            .field("gridfs", false)
                            .startObject("options")
                                .field("secondary_read_preference", "false")
                                .field("drop_collection", "true")
                                .field("is_mongos", "false")
                            .endObject()
                        .endObject()
                        .startObject("index")
                            .field("name", dbName)
                            .field("type", collectionName)
                            .field("bulk_size", "1000")
                            .field("bulk_timeout", "30")
                        .endObject()
                    .endObject();
            getClient().prepareIndex("_river", collectionName, "_meta").setSource(config)
                    .execute().actionGet(initTimeout);
            logger.info("Collection {}.{} indexing request sent to ES", dbName, collectionName);
        }
    }
}
