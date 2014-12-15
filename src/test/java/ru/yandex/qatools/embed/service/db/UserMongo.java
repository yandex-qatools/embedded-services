package ru.yandex.qatools.embed.service.db;

import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

/**
 * @author smecsia
 */
@Entity("users")
public class UserMongo {

    @Id
    private Long id;

    @Embedded
    private UserDetailMongo detail;

    private String name;

    public UserDetailMongo getDetail() {
        return detail;
    }

    public void setDetail(UserDetailMongo detail) {
        this.detail = detail;
    }

    public long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }
}
