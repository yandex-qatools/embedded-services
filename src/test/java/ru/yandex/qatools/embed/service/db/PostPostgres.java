package ru.yandex.qatools.embed.service.db;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

import java.sql.Timestamp;

@Table("posts")
public class PostPostgres extends Model {
    public String getText() {
        return getString("text");
    }

    public void setText(String text) {
        set("text", text);
    }

    public Timestamp getUpdatedAt() {
        return getTimestamp("updated_at");
    }

    public Timestamp getCreatedAt() {
        return getTimestamp("created_at");
    }

    public String getTitle() {
        return getString("title");
    }

    public void setTitle(String title) {
        set("title", title);
    }
}
