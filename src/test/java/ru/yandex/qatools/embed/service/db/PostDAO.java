package ru.yandex.qatools.embed.service.db;

import org.mongodb.morphia.dao.BasicDAO;

/**
 * @author smecsia
 */
public class PostDAO extends BasicDAO<PostMongo, String> {

    public PostDAO(MorphiaDBService dbService) {
        super(dbService.getDatastore());
    }
}
