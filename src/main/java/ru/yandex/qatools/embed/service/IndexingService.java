package ru.yandex.qatools.embed.service;

import ru.yandex.qatools.embed.service.beans.IndexingResult;

import java.util.List;

/**
 * @author Ilya Sadykov
 */
public interface IndexingService {
    List<IndexingResult> search(Class modelClass, String value);

    List<IndexingResult> search(String collectionName, String value);

    void addToIndex(Class modelClass);

    void indexAll();

    void addToIndex(String collectionName);

    String collectionName(Class modelClass);
}
