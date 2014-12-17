package ru.yandex.qatools.embed.service;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import ru.yandex.qatools.embed.service.beans.IndexingResult;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.util.Collections.newSetFromMap;
import static org.apache.commons.lang3.StringUtils.join;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.queryString;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * @author Ilya Sadykov
 */
public abstract class AbstractElasticEmbeddedService extends AbstractEmbeddedService implements IndexingService {
    protected final String dbName;
    protected volatile Node node;
    protected final Set<String> indexedCollections = newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    public AbstractElasticEmbeddedService(String dbName, String dataDirectory, boolean enabled, int initTimeout) throws IOException {
        super(dataDirectory, enabled, initTimeout);
        this.dbName = dbName;
    }

    protected abstract void indexAllCollections() throws IOException;

    protected abstract void indexCollection(String collectionName) throws IOException;

    @Override
    public void doStart() {
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("http.enabled", "false")
                .put("path.home", dataDirectory)
                .put("threadpool.bulk.queue_size", 5000)
                .put("path.data", dataDirectory + "/data")
                .put("path.logs", dataDirectory + "/logs");
        this.node = nodeBuilder().local(true).settings(elasticsearchSettings.build()).node();
    }

    @Override
    public void doStop() {
        if (node != null) {
            node.close();
            node = null;
        }
    }

    @Override
    public List<IndexingResult> search(Class modelClass, String value) {
        return search(collectionName(modelClass), value);
    }

    @Override
    public List<IndexingResult> search(String collectionName, String value) {
        final List<IndexingResult> results = new ArrayList<>();
        if (enabled) {
            logger.debug(format("Searching for '%s' in collection '%s' ...", value, collectionName));
            final SearchResponse resp = search(collectionName, queryString(value));
            for (SearchHit hit : resp.getHits()) {
                results.add(new IndexingResult(hit.getId(), hit.score(), hit.getSource()));
            }
            logger.debug(format("Search for '%s' in collection '%s' gave %d results...",
                    value, collectionName, results.size()));
        }
        return results;
    }

    @Override
    public void indexAll() {
        try {
            indexAllCollections();
        } catch (IOException e) {
            throw new RuntimeException("Failed to index all collections", e);
        }
    }

    @Override
    public void addToIndex(Class modelClass) {
        addToIndex(collectionName(modelClass));
    }

    @Override
    public void addToIndex(String collectionName) {
        try {
            if (indexedCollections.contains(collectionName)) {
                logger.debug(format("Skipping collection '%s' indexing: it is already added to index!", collectionName));
                return;
            }
            indexedCollections.add(collectionName);
            logger.debug(format("Adding collection '%s' to the embedded ElasticSearch index...", collectionName));
            indexCollection(collectionName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to index collection", e);
        }
    }

    @Override
    public void initSettings(Map<String, Object> settings) {
        if (enabled) {
            try {
                if (settings.isEmpty()) {
                    logger.info("Database {} skipping settings configuration", dbName);
                    return;
                }
                IndicesExistsResponse existsResp = getClient().admin().indices().prepareExists(dbName).execute().get();
                try {
                    if (existsResp.isExists()) {
                        logger.info("Index exists {}, removing...", dbName);
                        getClient().admin().indices().prepareDelete(dbName).execute().get();
                    }
                    logger.info("Creating settings index {}...", dbName);
                    final Map<String, Object> config = new HashMap<>();
                    Map<String, Object> currentMap = config;
                    for (String fieldPath : settings.keySet()) {
                        String[] parts = fieldPath.split("\\.");
                        for (int i = 0; i < parts.length; ++i) {
                            if ((i < parts.length - 1)) {
                                currentMap.put(parts[i], (currentMap = new HashMap<>()));
                            } else {
                                currentMap.put(parts[i], settings.get(fieldPath));
                            }
                        }
                    }
                    getClient().admin().indices().prepareCreate(dbName).setSettings(config).execute().actionGet(initTimeout);
                    logger.info("Settings index {} creation sent to ES", dbName);
                } catch (Exception e) {
                    logger.error("Failed to create settings index {}", dbName, e);
                }
                logger.info("Database {} settings requests sent to ES", dbName);
            } catch (Exception e) {
                logger.error("Failed to setup settings for db {}", dbName, e);
            }
        }

    }

    private void openIndex() {
        try {
            getClient().admin().indices().prepareOpen(dbName).execute().actionGet(initTimeout);
        } catch (Exception e) {
            logger.error("Failed to open index {}", dbName, e);
        }
    }

    private void closeIndex() {
        try {
            getClient().admin().indices().prepareClose(dbName).execute().actionGet(initTimeout);
        } catch (Exception e) {
            logger.error("Failed to close index {}", dbName, e);
        }
    }

    private void createIndexIfNotExists() throws InterruptedException, java.util.concurrent.ExecutionException {
        IndicesExistsResponse existsResp = getClient().admin().indices().prepareExists(dbName).execute().get();
        if (!existsResp.isExists()) {
            logger.info("Index does not exists {}, creating...", dbName);
            getClient().admin().indices().prepareCreate(dbName).execute().actionGet(initTimeout);
        }
    }


    @Override
    public void initMappings(Map<String, Map<String, Object>> typedFields) {
        if (enabled) {
            try {
                if (typedFields.isEmpty()) {
                    logger.info("Database {} skipping mapping configuration", dbName);
                    return;
                }
                createIndexIfNotExists();
                for (String fieldPath : typedFields.keySet()) {
                    String[] parts = fieldPath.split("\\.");
                    String type = parts[0];

                    GetMappingsResponse getResp = getClient().admin().indices().prepareGetMappings(dbName)
                            .setTypes(type).execute().get();
                    if (getResp.getMappings().containsKey(type)) {
                        logger.info("Mapping index already exists {}/{}...", dbName, fieldPath);
                        try {
                            logger.info("Deleting mapping index {}/{}...", dbName, fieldPath);
                            getClient().admin().indices().prepareDeleteMapping(dbName).setType(type).execute().get();
                        } catch (Exception e) {
                            logger.error("Failed to delete mapping index {}/{}", dbName, fieldPath, e);
                        }
                    }

                    try {
                        logger.info("Creating mapping index {}/{}...", dbName, fieldPath);
                        final PutMappingRequestBuilder putBuilder = getClient().admin().indices().preparePutMapping(dbName);
                        final XContentBuilder config = jsonBuilder()
                                .startObject().
                                        startObject(type);
                        for (int i = 1; i < parts.length; ++i) {
                            config
                                    .startObject("properties")
                                    .startObject(parts[i]);
                            final Map<String, Object> opts = typedFields.get(fieldPath);
                            if (i < parts.length - 1) {
                                config.field("type", "nested");
                            } else {
                                for (String opt : opts.keySet()) {
                                    config.field(opt, opts.get(opt));
                                }
                            }
                        }
                        for (String part : parts) {
                            config
                                    .endObject()
                                    .endObject();
                        }
                        putBuilder.setType(type).setSource(config);
                        putBuilder.execute().actionGet(initTimeout);
                        logger.info("Mapping index {}/{}: {} creation sent to ES", dbName, fieldPath, join(typedFields.get(fieldPath)));
                    } catch (Exception e) {
                        logger.error("Failed to create mapping index {}/{}", dbName, fieldPath, e);
                    }
                }
                logger.info("Database {} mapping requests sent to ES", dbName);
            } catch (Exception e) {
                logger.error("Failed to setup mappings for db {}", dbName, e);
            }
        }
    }

    public Client getClient() {
        return node.client();
    }

    protected SearchResponse search(String collectionName, QueryBuilder query) {
        final CountResponse count = count(collectionName, query);
        return getClient().prepareSearch().setTypes(collectionName)
                .setQuery(query)
                .setSize((int) count.getCount())
                .addFields("id")
                .execute()
                .actionGet();
    }

    protected CountResponse count(String collectionName, QueryBuilder query) {
        return getClient().prepareCount()
                .setTypes(collectionName)
                .setQuery(query)
                .execute()
                .actionGet();
    }

    @Override
    public String collectionName(Class modelClass) {
        return modelClass.getSimpleName().toLowerCase();
    }


}
