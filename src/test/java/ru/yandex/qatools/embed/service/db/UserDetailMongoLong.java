package ru.yandex.qatools.embed.service.db;

import org.mongodb.morphia.annotations.Embedded;

/**
 * @author smecsia
 */
@Embedded
public class UserDetailMongoLong implements UserDetailMongo {

    private Long _id;

    public UserDetailMongoLong(Long _id) {
        this._id = _id;
    }

    public UserDetailMongoLong() {
    }

    public Long get_id() {
        return _id;
    }

    public void set_id(Long _id) {
        this._id = _id;
    }
}
