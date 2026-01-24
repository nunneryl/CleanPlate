-- schema.sql
-- CleanPlate Production Database Schema
-- Last updated based on production database inspection

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- ============================================================================
-- RESTAURANTS TABLE
-- Core table storing NYC restaurant inspection data with enrichment fields
-- ============================================================================
CREATE TABLE IF NOT EXISTS restaurants (
    camis VARCHAR NOT NULL,
    dba VARCHAR,
    boro VARCHAR,
    building VARCHAR,
    street VARCHAR,
    zipcode VARCHAR,
    phone VARCHAR,
    inspection_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    critical_flag VARCHAR,
    record_date TIMESTAMP WITHOUT TIME ZONE,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    community_board VARCHAR,
    council_district VARCHAR,
    census_tract VARCHAR,
    bin VARCHAR,
    bbl VARCHAR,
    nta VARCHAR,
    cuisine_description VARCHAR,
    grade TEXT,
    grade_date TIMESTAMP WITHOUT TIME ZONE,
    score INTEGER,
    violation_code VARCHAR,
    violation_description TEXT,
    inspection_type VARCHAR,
    dba_tsv TSVECTOR,
    dba_normalized_search TEXT,
    action TEXT,
    -- Third-party enrichment IDs
    foursquare_fsq_id TEXT,
    google_place_id TEXT,
    yelp_business_id TEXT,
    -- Google Places enrichment data
    google_rating NUMERIC,
    google_review_count INTEGER,
    website TEXT,
    hours JSONB,
    google_maps_url TEXT,
    price_level TEXT,
    google_id_last_checked TIMESTAMP WITH TIME ZONE,
    enrichment_last_attempted TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (camis, inspection_date)
);

-- ============================================================================
-- VIOLATIONS TABLE
-- Stores individual violation records linked to restaurant inspections
-- ============================================================================
CREATE TABLE IF NOT EXISTS violations (
    id SERIAL PRIMARY KEY,
    camis VARCHAR,
    inspection_date TIMESTAMP WITHOUT TIME ZONE,
    violation_code VARCHAR,
    violation_description TEXT
);

-- ============================================================================
-- USERS TABLE
-- Stores Apple Sign In user identifiers
-- ============================================================================
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- FAVORITES TABLE
-- Stores user's favorite restaurants
-- ============================================================================
CREATE TABLE IF NOT EXISTS favorites (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    restaurant_camis VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, restaurant_camis)
);

-- ============================================================================
-- RECENT SEARCHES TABLE
-- Stores user's recent search history
-- ============================================================================
CREATE TABLE IF NOT EXISTS recent_searches (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    search_term_normalized TEXT NOT NULL,
    search_term_display TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, search_term_normalized)
);

-- ============================================================================
-- GRADE UPDATES TABLE
-- Tracks when restaurant grades change (new grades or finalized pending grades)
-- ============================================================================
CREATE TABLE IF NOT EXISTS grade_updates (
    id SERIAL PRIMARY KEY,
    restaurant_camis VARCHAR NOT NULL,
    previous_grade VARCHAR,
    new_grade VARCHAR,
    update_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    update_type VARCHAR,
    inspection_date DATE
);

-- ============================================================================
-- INDEXES
-- Performance optimization indexes
-- ============================================================================

-- Restaurant search and filter indexes
CREATE INDEX IF NOT EXISTS idx_restaurants_dba_normalized_search ON restaurants USING gin (dba_normalized_search gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_restaurants_grade ON restaurants (grade);
CREATE INDEX IF NOT EXISTS idx_restaurants_boro ON restaurants (boro);
CREATE INDEX IF NOT EXISTS idx_restaurants_dba ON restaurants USING gin (dba gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_restaurants_zipcode ON restaurants (zipcode);

-- User-related table indexes
CREATE INDEX IF NOT EXISTS idx_favorites_user_id ON favorites (user_id);
CREATE INDEX IF NOT EXISTS idx_recent_searches_user_id ON recent_searches (user_id);

-- Grade updates indexes
CREATE INDEX IF NOT EXISTS idx_grade_updates_restaurant ON grade_updates (restaurant_camis, inspection_date);
CREATE INDEX IF NOT EXISTS idx_grade_updates_date ON grade_updates (update_date);
