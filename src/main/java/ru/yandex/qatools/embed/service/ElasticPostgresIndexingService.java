package ru.yandex.qatools.embed.service;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.sql.Driver;

import static java.lang.String.format;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticPostgresIndexingService extends AbstractElasticEmbeddedService implements IndexingService {
    protected final String host;
    protected final int port;
    protected final String dbName;
    protected final String username;
    protected final String password;
    private final Class<? extends Driver> driverClass;
    private final String driverProto;
    private final String driverOpts;

    public ElasticPostgresIndexingService(Class<? extends Driver> driverClass,
                                          String driverProto, String driverOpts,
                                          String host, int port, String username, String password,  String dbName,
                                          String dataDirectory, boolean enabled, int initTimeout) throws IOException {
        super(dataDirectory, enabled, initTimeout);
        this.username = username;
        this.password = password;
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.driverClass = driverClass;
        this.driverOpts = driverOpts;
        this.driverProto = driverProto;
    }

    @Override
    protected void indexCollection(String collectionName) throws IOException {
        if (enabled) {
            final XContentBuilder config = jsonBuilder()
                    .startObject()
                    .field("type", "jdbc")
                        .startObject("jdbc")
                            .field("driver", driverClass.getName())
                            .field("url", format("jdbc:%s://%s:%s/%s%s", driverProto, host, port, dbName, driverOpts))
                            .field("user", username)
                            .field("password", password)
                            .field("index", "index")
                            .field("type", collectionName)
                            .field("strategy", "simple")
                            .field("sql", format("select id as _id, * from %s ", collectionName))
                        .endObject()
                        .startObject("index")
                            .field("index", collectionName)
                            .field("type", collectionName)
                            .field("bulk_size", "1000")
                            .field("bulk_timeout", "30")
                        .endObject()
                    .endObject();
            getClient().prepareIndex("_river", collectionName, "_meta").setSource(config)
                    .execute().actionGet(initTimeout);
        }
    }
}
