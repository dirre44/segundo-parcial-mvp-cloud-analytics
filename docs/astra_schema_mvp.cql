-- Segundo Parcial — MVP — Esquema AstraDB
-- Keyspace nuevo dentro de la misma base de datos del proyecto final.
-- Correr en la CQL Console de Astra.

CREATE KEYSPACE IF NOT EXISTS mvp_parcial
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Tabla única requerida por la consigna: mart FinOps org_daily_usage_by_service
-- Query-first: PRIMARY KEY pensada para "uso/costo de una org, en un rango de fechas".
CREATE TABLE IF NOT EXISTS mvp_parcial.org_daily_usage_by_service (
    org_id text,
    usage_date date,
    service text,
    event_count int,
    total_value double,
    requests_count int,
    total_cost_usd double,
    total_carbon_kg double,
    PRIMARY KEY (org_id, usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
