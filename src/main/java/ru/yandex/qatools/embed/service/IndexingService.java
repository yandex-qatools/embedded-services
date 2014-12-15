package ru.yandex.qatools.embed.service;

import ru.yandex.qatools.embed.service.beans.IndexingResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Ilya Sadykov
 */
public interface IndexingService {
    void setupMappings(Map<String, String> typedFields) throws IOException;

    List<IndexingResult> search(Class modelClass, String value);

    List<IndexingResult> search(String collectionName, String value);

    void addToIndex(Class modelClass);

    void indexAll();

    void addToIndex(String collectionName);

    String collectionName(Class modelClass);
}
