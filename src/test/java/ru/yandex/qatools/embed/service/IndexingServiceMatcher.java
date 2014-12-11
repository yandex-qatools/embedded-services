package ru.yandex.qatools.embed.service;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class IndexingServiceMatcher<E> extends TypeSafeMatcher<IndexingService> {

    private final Class entityClass;
    private final String query;
    private final MatchFunction<Boolean, IndexingService> callable;

    private IndexingServiceMatcher(Class entityClass, String query,
                                   MatchFunction<Boolean, IndexingService> callable) {
        this.entityClass = entityClass;
        this.query = query;
        this.callable = callable;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(String.format(
                "Repository containing some %s found by query '%s'",
                entityClass.getSimpleName(), query));
    }

    @Override
    protected boolean matchesSafely(IndexingService repo) {
        try {
            return callable.call(repo);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static interface MatchFunction<R, P> {
        public R call(P arg);
    }

    public static <T> IndexingServiceMatcher<T> findIndexedAny(
            final Class<T> entityClass, final String query) {
        return new IndexingServiceMatcher<>(entityClass, query, new MatchFunction<Boolean, IndexingService>() {
            @Override
            public Boolean call(IndexingService service) {
                return service.search(entityClass, query).size() > 0;
            }
        });
    }

    public static <T> IndexingServiceMatcher<T> findIndexedAtLeast(final Class<T> entityClass,
                                                                   final String query, final int count) {
        return new IndexingServiceMatcher<>(entityClass, query, new MatchFunction<Boolean, IndexingService>() {
            @Override
            public Boolean call(IndexingService service) {
                return service.search(entityClass, query).size() >= count;
            }
        });
    }

    public static <T> IndexingServiceMatcher<T> findIndexedAtLeast(final Class<T> entityClass, final String collection,
                                                                   final String query, final int count) {
        return new IndexingServiceMatcher<>(entityClass, query, new MatchFunction<Boolean, IndexingService>() {
            @Override
            public Boolean call(IndexingService service) {
                return service.search(collection, query).size() >= count;
            }
        });
    }

}