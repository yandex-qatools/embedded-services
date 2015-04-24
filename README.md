# Embedded services
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/ru.yandex.qatools.embed/embedded-services/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/ru.yandex.qatools.embed/embedded-services) [![covarage](https://img.shields.io/sonar/http/sonar.qatools.ru/ru.yandex.qatools.embed:embedded-services/coverage.svg?style=flat)](http://sonar.qatools.ru/dashboard/index/887)

This project allows you to easily start your project with the embedded database (PostgreSQL, MongoDB) services and connect 
them with the embedded ElasticSearch instance for full text indexing and search.

## Why?

It's very easy to incorporate the embedded MongoDB/PostgreSQL within your test process.

### Maven

Add the following dependency to your pom.xml:
```xml
    <dependency>
        <groupId>ru.yandex.qatools.embed</groupId>
        <artifactId>embedded-services</artifactId>
        <version>1.15</version>
    </dependency>
```
## How to run embedded MongoDB with ElasticSearch

```java
        // Starting the embedded services within temporary dir
        MongoEmbeddedService mongo = new MongoEmbeddedService(
                "localhost:27017", "dbname", "username", "password", "localreplica"
        );
        mongo.start();
        ElasticMongoIndexingService elastic = new ElasticMongoIndexingService(
                "localhost:27017", "dbname", "username", "password"
        );
        elastic.start();
        
        // Indexing collection `posts`
        elastic.addToIndex("posts");
        
        // Searching within collection `posts` using Elastic (IndexingResult contains id of each post)
        List<IndexingResult> posts = elastic.search("posts", "body:(lorem AND NOT ipsum)")
```

## How to run embedded PostgreSQL with ElasticSearch

```java
        // Starting the embedded services within temporary dir
        PostgresEmbeddedService postgres = new PostgresEmbeddedService(
                "localhost", 5429, "username", "password",  "dbname"
        );
        postgres.start();
        ElasticPostgresIndexingService elastic = new ElasticPostgresIndexingService(
                Driver.class, "postgresql", "", "localhost", 5429, "username", "password", "dbname"
        );
        elastic.start();
        
        // Indexing table `posts`
        elastic.addToIndex("posts");
        
        // Searching within table `posts` using Elastic (IndexingResult contains id of each post)
        List<IndexingResult> posts = elastic.search("posts", "body:(lorem AND NOT ipsum)")
```
