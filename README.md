# Embedded services

This project allows you to easily start your project with the embedded database (PostgreSQL, MongoDB) services and connect 
them with the embedded ElasticSearch instance for better full text indexing and search.

## Mongo with ElasticSearch

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

## Postgres with ElasticSearch

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
