package ru.yandex.qatools.embed.service.db;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

/**
 * @author smecsia
 */
@Entity("posts")
public class PostMongo {

    @Id
    private ObjectId id;

    @Embedded
    UserMongo user;

    private String title;

    private String body;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public UserMongo getUser() {
        return user;
    }

    public void setUser(UserMongo user) {
        this.user = user;
    }
}
