-- Benchmark schema: client + client_status + client_address.
-- Applied with: make schema

CREATE DATABASE IF NOT EXISTS bench;

USE bench;

CREATE TABLE IF NOT EXISTS client (
    id          UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    name        STRING NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS client_status (
    id          UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    client_id   UUID NOT NULL REFERENCES client(id),
    status      STRING NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    INDEX idx_client_status_client_id (client_id)
);

CREATE TABLE IF NOT EXISTS client_address (
    id          UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    client_id   UUID NOT NULL REFERENCES client(id),
    street      STRING NOT NULL,
    city        STRING NOT NULL,
    country     STRING NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    INDEX idx_client_address_client_id (client_id)
);
