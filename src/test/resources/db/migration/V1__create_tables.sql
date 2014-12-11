CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    text  VARCHAR(1000000) NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    extra_details json
);
