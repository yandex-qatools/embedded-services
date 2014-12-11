package ru.yandex.qatools.embed.service.db;

import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.javalite.activejdbc.Base;
import org.postgresql.ds.PGPoolingDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import static java.lang.String.format;

public class Database {
    private final static Logger logger = LoggerFactory.getLogger(Database.class);
    private final String dbUrl;
    private final String username;
    private final String password;
    private int maxPoolSize;

    public Database(String host, int port, String dbName, String username, String password, int maxPoolSize) {
        this.dbUrl = format("jdbc:postgresql://%s:%s/%s?user=%s&password=%s", host, port, dbName, username, password);
        this.username = username;
        this.password = password;
        this.maxPoolSize = maxPoolSize;
    }

    public void connect() {
        try {
            logger.info(format("Connecting to database with url '%s' ...", dbUrl));
            openConnection();
            Flyway flyway = new Flyway();
            flyway.setDataSource(dataSource());
            flyway.migrate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start database", e);
        }
    }

    private DataSource dataSource() {
        final HikariDataSource ds = new HikariDataSource();
        ds.setMaximumPoolSize(maxPoolSize);
        ds.setDataSourceClassName(PGPoolingDataSource.class.getName());
        ds.addDataSourceProperty("url", dbUrl);
        ds.addDataSourceProperty("user", username);
        ds.addDataSourceProperty("password", password);
        return ds;
    }

    public void disconnect() {
        closeConnection();
    }

    public void openConnection() {
        if (!Base.hasConnection()) {
            Base.open(dataSource());
        }
    }

    public void closeConnection() {
        if (Base.hasConnection()) {
            Base.close();
        }
    }
}
