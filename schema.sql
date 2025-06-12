-- schema.sql

-- Enable extensions needed for search. These commands will only run if the
-- extensions are not already enabled, so they are safe to run every time.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- Create the 'restaurants' table if it doesn't already exist.
-- This table holds the main information for each restaurant inspection.
CREATE TABLE IF NOT EXISTS restaurants (
    camis INT NOT NULL,
    inspection_date DATE NOT NULL,
    dba TEXT,
    dba_normalized_search TEXT, -- For case-insensitive and accent-insensitive search
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
    -- Define a composite primary key. A restaurant (camis) can have multiple
    -- inspections on different dates. This combination must be unique.
    PRIMARY KEY (camis, inspection_date)
);

-- Create the 'violations' table if it doesn't already exist.
-- This table holds the specific violations for each inspection.
CREATE TABLE IF NOT EXISTS violations (
    id SERIAL PRIMARY KEY, -- Simple auto-incrementing primary key
    camis INT NOT NULL,
    inspection_date DATE NOT NULL,
    violation_code VARCHAR(10),
    violation_description TEXT,
    -- Create a foreign key to link violations back to a specific inspection
    -- in the 'restaurants' table. This ensures data integrity.
    FOREIGN KEY (camis, inspection_date) REFERENCES restaurants (camis, inspection_date) ON DELETE CASCADE
);

-- Create indexes to speed up common queries. This is crucial for performance.

-- Index for the main search field (dba_normalized_search) using GIN,
-- which is optimized for trigram-based fuzzy text search.
CREATE INDEX IF NOT EXISTS idx_restaurants_dba_normalized_search ON restaurants USING gin (dba_normalized_search gin_trgm_ops);

-- A standard index on the 'grade' column for faster filtering by grade.
CREATE INDEX IF NOT EXISTS idx_restaurants_grade ON restaurants (grade);

-- A standard index on the 'boro' column for faster filtering by boro.
CREATE INDEX IF NOT EXISTS idx_restaurants_boro ON restaurants (boro);