package ru.yandex.qatools.embed.service.db;

import org.mongodb.morphia.dao.BasicDAO;

/**
 * @author smecsia
 */
public class UserDAO extends BasicDAO<UserMongo, String> {

    public UserDAO(MorphiaDBService dbService) {
        super(dbService.getDatastore());
    }
}
