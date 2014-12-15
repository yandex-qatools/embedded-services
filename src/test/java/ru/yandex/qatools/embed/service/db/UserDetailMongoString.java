package ru.yandex.qatools.embed.service.db;

import org.mongodb.morphia.annotations.Embedded;

/**
 * @author smecsia
 */
@Embedded
public class UserDetailMongoString implements UserDetailMongo {

    private String _id;

    public UserDetailMongoString(String _id) {
        this._id = _id;
    }

    public UserDetailMongoString() {
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }
}
