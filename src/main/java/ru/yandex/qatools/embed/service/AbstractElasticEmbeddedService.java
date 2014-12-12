package ru.yandex.qatools.embed.service;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import ru.yandex.qatools.embed.service.beans.IndexingResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.util.Collections.newSetFromMap;
import static org.elasticsearch.index.query.QueryBuilders.queryString;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * @author Ilya Sadykov
 */
public abstract class AbstractElasticEmbeddedService extends AbstractEmbeddedService implements IndexingService {
    protected volatile Node node;
    protected final Set<String> indexedCollections = newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    public AbstractElasticEmbeddedService(String dataDirectory, boolean enabled, int initTimeout) throws IOException {
        super(dataDirectory, enabled, initTimeout);
    }

    protected abstract void indexCollection(String collectionName) throws IOException;

    @Override
    public void doStart() {
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("http.enabled", "false")
                .put("path.home", dataDirectory)
                .put("path.data", dataDirectory + "/data")
                .put("path.logs", dataDirectory + "/logs");
        this.node = nodeBuilder().local(true).settings(elasticsearchSettings.build()).node();
    }

    @Override
    public void doStop() {
        if (node != null) {
            node.stop();
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


    public Client getClient() {
        return node.client();
    }

    private SearchResponse search(String collectionName, QueryBuilder query) {
        final CountResponse count = count(collectionName, query);
        return getClient().prepareSearch().setTypes(collectionName)
                .setQuery(query)
                .setSize((int) count.getCount())
                .addFields("id")
                .execute()
                .actionGet();
    }

    private CountResponse count(String collectionName, QueryBuilder query) {
        return getClient().prepareCount().setTypes(collectionName)
                .setQuery(query)
                .execute()
                .actionGet();
    }

    @Override
    public String collectionName(Class modelClass) {
        return modelClass.getSimpleName().toLowerCase();
    }


}
