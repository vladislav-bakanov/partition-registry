CREATE SCHEMA IF NOT EXISTS registry;

-- Registry for sources
CREATE TABLE IF NOT EXISTS registry.sources (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    owner TEXT,
    access_key TEXT NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Registry for providers
CREATE TABLE IF NOT EXISTS registry.providers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    access_key TEXT NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Registry for partitions
CREATE TABLE IF NOT EXISTS registry.partitions (
    id SERIAL PRIMARY KEY,
    start TIMESTAMPTZ NOT NULL,
    "end" TIMESTAMPTZ NOT NULL,
    source_id INTEGER NOT NULL,
    provider_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES registry.sources(id),
    FOREIGN KEY (provider_id) REFERENCES registry.providers(id)
);

-- Registry for partition events
CREATE TABLE IF NOT EXISTS registry.events (
    partition_id INT,
    event_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (partition_id) REFERENCES registry.partitions(id)
);
