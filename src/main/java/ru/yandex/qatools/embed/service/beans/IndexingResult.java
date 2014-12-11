package ru.yandex.qatools.embed.service.beans;

import java.util.Map;

/**
 * @author smecsia
 */
public class IndexingResult {

    final String id;
    final float score;
    final Map<String, Object> attrs;

    public IndexingResult(String id, float score, Map<String, Object> attrs) {
        this.id = id;
        this.score = score;
        this.attrs = attrs;
    }

    public String getId() {
        return id;
    }

    public float getScore() {
        return score;
    }

    public Map<String, Object> getAttrs() {
        return attrs;
    }
}
