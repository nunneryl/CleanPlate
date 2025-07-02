-- schema.sql

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE TABLE IF NOT EXISTS restaurants (
    camis INT NOT NULL,
    inspection_date DATE NOT NULL,
    dba TEXT,
    dba_normalized_search TEXT,
    boro TEXT,
    building TEXT,
    street TEXT,
    zipcode TEXT,
    phone TEXT,
    cuisine_description TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    critical_flag TEXT,
    grade CHAR(1),
    grade_date DATE,
    inspection_type TEXT,
    PRIMARY KEY (camis, inspection_date)
);

CREATE TABLE IF NOT EXISTS violations (
    id SERIAL PRIMARY KEY,
    camis INT NOT NULL,
    inspection_date DATE NOT NULL,
    violation_code VARCHAR(10),
    violation_description TEXT
);

-- Indexes to speed up new filter and search queries.
CREATE INDEX IF NOT EXISTS idx_restaurants_dba_normalized_search ON restaurants USING gin (dba_normalized_search gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_restaurants_grade ON restaurants (grade);
CREATE INDEX IF NOT EXISTS idx_restaurants_boro ON restaurants (boro);