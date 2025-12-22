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

-- Users table for Apple Sign In authentication
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Favorites table to store user's favorite restaurants
CREATE TABLE IF NOT EXISTS favorites (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    restaurant_camis INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (user_id, restaurant_camis)
);

-- Recent searches table to store user's search history
CREATE TABLE IF NOT EXISTS recent_searches (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    search_term_display TEXT NOT NULL,
    search_term_normalized TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (user_id, search_term_normalized)
);

-- Grade updates table to track when pending grades are finalized
CREATE TABLE IF NOT EXISTS grade_updates (
    id SERIAL PRIMARY KEY,
    restaurant_camis INT NOT NULL,
    inspection_date DATE NOT NULL,
    update_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    update_type TEXT NOT NULL
);

-- Indexes to speed up new filter and search queries.
CREATE INDEX IF NOT EXISTS idx_restaurants_dba_normalized_search ON restaurants USING gin (dba_normalized_search gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_restaurants_grade ON restaurants (grade);
CREATE INDEX IF NOT EXISTS idx_restaurants_boro ON restaurants (boro);

-- Indexes for user-related tables
CREATE INDEX IF NOT EXISTS idx_favorites_user_id ON favorites (user_id);
CREATE INDEX IF NOT EXISTS idx_recent_searches_user_id ON recent_searches (user_id);
CREATE INDEX IF NOT EXISTS idx_grade_updates_restaurant ON grade_updates (restaurant_camis, inspection_date);
CREATE INDEX IF NOT EXISTS idx_grade_updates_date ON grade_updates (update_date);