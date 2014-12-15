package ru.yandex.qatools.embed.service;

import org.elasticsearch.common.xcontent.XContentBuilder;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.sql.Driver;

import static java.lang.String.format;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticPostgresIndexingService extends AbstractElasticEmbeddedService implements IndexingService {
    protected final String host;
    protected final int port;
    protected final String username;
    protected final String password;
    protected final Class<? extends Driver> driverClass;
    protected final String driverProto;
    protected final String driverOpts;

    public ElasticPostgresIndexingService(Class<? extends Driver> driverClass,
                                          String driverProto, String driverOpts,
                                          String host, int port, String username, String password,  String dbName) throws IOException {
        this(driverClass, driverProto, driverOpts, host, port, username, password, dbName, null, true, 10000);
    }

    public ElasticPostgresIndexingService(Class<? extends Driver> driverClass,
                                          String driverProto, String driverOpts,
                                          String host, int port, String username, String password,  String dbName,
                                          String dataDirectory, boolean enabled, int initTimeout) throws IOException {
        super(dbName, dataDirectory, enabled, initTimeout);
        this.username = username;
        this.password = password;
        this.host = host;
        this.port = port;
        this.driverClass = driverClass;
        this.driverOpts = driverOpts;
        this.driverProto = driverProto;
    }

    @Override
    protected void indexAllCollections() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    protected synchronized void indexCollection(String tableName) throws IOException {
        if (enabled) {
            final XContentBuilder config = jsonBuilder()
                    .startObject()
                    .field("type", "jdbc")
                        .startObject("jdbc")
                            .field("driver", driverClass.getName())
                            .field("url", formatConnectionUrl())
                            .field("user", username)
                            .field("password", password)
                            .field("index", "index")
                            .field("type", tableName)
                            .field("strategy", "simple")
                            .field("sql", formatIndexQuery(tableName))
                        .endObject()
                        .startObject("index")
                            .field("index", tableName)
                            .field("type", tableName)
                            .field("bulk_size", "1000")
                            .field("bulk_timeout", "30")
                        .endObject()
                    .endObject();
            getClient().prepareDelete("_river", tableName, "_meta").execute().actionGet(initTimeout);
            getClient().prepareIndex("_river", tableName, "_meta").setSource(config)
                    .execute().actionGet(initTimeout);
            logger.info("Table {}.{} indexing request sent to ES", dbName, tableName);
        }
    }

    protected String formatConnectionUrl() {
        return format("jdbc:%s://%s:%s/%s%s", driverProto, host, port, dbName, driverOpts);
    }

    protected String formatIndexQuery(String collectionName) {
        return format("select id as _id, * from %s ", collectionName);
    }
}
