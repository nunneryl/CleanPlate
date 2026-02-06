# CleanPlate Development Roadmap

> **Last Updated:** February 6, 2026
> **Status:** Active Development
> **Current Version:** 2.13.0

---

## Repositories

| Repo | Purpose |
|------|---------|
| `CleanPlate` (this repo) | Backend API, database, cron jobs |
| `nunneryl/CleanPlate-IOS` | iOS SwiftUI app |

---

## Priority Legend

| Priority | Label | Description |
|----------|-------|-------------|
| 🔴 P0 | **CRITICAL** | Bugs affecting data integrity or user trust - fix immediately |
| 🟠 P1 | **HIGH** | Performance issues or significant UX problems |
| 🟡 P2 | **MEDIUM** | Important improvements for user experience |
| 🟢 P3 | **LOW** | Nice-to-have features and optimizations |
| 🔵 P4 | **FUTURE** | Strategic features for later phases |

---

## ✅ COMPLETED

### P0 Bugs - All Fixed

| # | Issue | Status | Location |
|---|-------|--------|----------|
| 1 | Account Deletion Cache Bug | ✅ Fixed | `app_search.py:584` - `cache.delete(f"user_{user_id}_/favorites")` |
| 2 | Grade Updates Sorting Bug | ✅ Fixed | `app_search.py:426` - `ORDER BY r.grade_date DESC` |
| 3 | "Grade Pending" in Recently Graded Filters | ✅ Fixed | iOS `RecentlyGradedListView.swift` - `.filter { $0 != .pending }` |

### P1 Items - Fixed

| # | Issue | Status | Details |
|---|-------|--------|---------|
| 5 | CAST Preventing Index Usage | ✅ Fixed | `update_database.py:111` uses `::date` syntax |
| 6 | Map Integration Reliability | ✅ Fixed | iOS `MapServices.swift` has 500m verification + fallback logic |

### P2 Items - Fixed

| # | Issue | Status | Details |
|---|-------|--------|---------|
| 9 | Error Recovery & Retry Logic | ✅ Fixed | iOS `APIService.swift` has exponential backoff (2^n seconds, 3 attempts) |

### P4 Items - Partially Complete

| # | Issue | Status | Details |
|---|-------|--------|---------|
| 16 | Google Data Integration | ✅ Partial | iOS displays ratings, hours, price level, website. Data exists in DB from prior $200 API spend. |

---

## 🟠 P1: HIGH PRIORITY - Performance & Reliability

### 4. Missing Database Indexes (Critical Performance)
**Status:** ⏳ Not Yet Applied
**Impact:** 3-10x slower queries on main endpoints

The following indexes are recommended but NOT in `schema.sql`:

```sql
-- CRITICAL: Violations join (80-90% faster searches)
CREATE INDEX IF NOT EXISTS idx_violations_camis_inspection_date
ON violations (camis, inspection_date);

-- HIGH: Action column filtering (60-70% faster recent-actions)
CREATE INDEX IF NOT EXISTS idx_restaurants_action
ON restaurants (action);

-- HIGH: Grade updates time filtering (40-50% faster)
CREATE INDEX IF NOT EXISTS idx_grade_updates_dates
ON grade_updates (update_date DESC, inspection_date DESC);

-- MEDIUM: Violation pruning
CREATE INDEX IF NOT EXISTS idx_violations_inspection_date
ON violations (inspection_date);

-- OPTIONAL: Optimize DISTINCT ON queries
CREATE INDEX IF NOT EXISTS idx_restaurants_camis_inspection_date_desc
ON restaurants (camis, inspection_date DESC);
```

**Effort:** 15 minutes to run in Railway PostgreSQL console

---

### 7. Apple Token Refresh Mechanism
**Status:** ⚠️ Still Needed
**Impact:** Users forced to re-authenticate when token expires (~24 hours)

**Current Behavior:**
- Token stored in Keychain
- On 401 response, user must manually re-authenticate with Apple

**Recommended Fix:**
1. Detect 401 in `APIService.swift`
2. Trigger Apple Sign-In re-auth silently or with minimal UI
3. Retry the failed request with new token

**Files:**
- `AuthenticationManager.swift` - Add refresh/re-auth logic
- `APIService.swift` - Add 401 interception with retry

**Effort:** 2-4 hours

---

## 🟡 P2: MEDIUM PRIORITY - User Experience

### 8. Offline Mode / Local Database
**Status:** 🆕 Not Started
**Impact:** App unusable without network; favorites not cached locally

**Implementation:**
1. Add Core Data or SwiftData for local persistence
2. Cache favorites locally for offline viewing
3. Cache recent searches
4. Sync when network available
5. Show "offline" indicator

**Files to Create/Update:**
- `PersistenceController.swift` (new)
- `FavoriteEntity.xcdatamodeld` (new)
- `AuthenticationManager.swift` - Add local sync
- `SearchViewModel.swift` - Cache results

**Effort:** 1-2 days

---

### 10. Deep Linking / Universal Links
**Status:** 🆕 Not Started
**Impact:** Can't share restaurant links that open in app

**Implementation:**
1. Configure Apple App Site Association file on backend
2. Add URL handling in `NYCFoodRatingsApp.swift`
3. Create shareable URLs: `cleanplate.app/restaurant/{camis}`
4. Handle incoming links and navigate to detail view

**Files:**
- `apple-app-site-association` (new, hosted on backend)
- `NYCFoodRatingsApp.swift` - Add `.onOpenURL` handler
- Backend: Add redirect endpoint

**Effort:** 4-6 hours

---

### 11. N+1 Query Fix in Backfill Script
**Status:** ⚡ Performance Issue
**File:** `backfill_grade_updates.py`

**Current:** Loops through restaurants with individual queries

**Fix:** Single query with window function:
```sql
WITH grade_sequences AS (
    SELECT camis, grade,
           LAG(grade) OVER (PARTITION BY camis ORDER BY inspection_date) as prev_grade
    FROM restaurants
)
SELECT DISTINCT ON (camis) camis, prev_grade, grade
FROM grade_sequences
WHERE prev_grade IN ('P', 'Z', 'N') AND grade IN ('A', 'B', 'C')
```

**Effort:** 30 minutes

---

## 🟢 P3: LOW PRIORITY - Enhancements

### 12. Frontend Defensive Sorting
**Status:** 🛡️ Optional
**Impact:** Ensures correct display even if backend sends unsorted data

**File:** `RecentlyGradedListViewModel.swift`

**Add:**
```swift
self.recentActivity = actionResults.recently_graded.sorted { r1, r2 in
    let date1 = r1.grade_date ?? "0000-00-00"
    let date2 = r2.grade_date ?? "0000-00-00"
    return date1 > date2
}
```

**Effort:** 15 minutes

---

### 13. Schema Cleanup - Remove Deprecated Columns
**Status:** 🧹 Tech Debt
**Impact:** Cleaner schema, less confusion

**Issues:**
1. `restaurants.violation_code/description` duplicates `violations` table
2. `dine_in`, `takeout`, `delivery` columns are unused (feature cancelled)

**Fix:**
1. Verify all code uses `violations` table only
2. Drop redundant columns from `restaurants` table:
   - `violation_code`
   - `violation_description`
   - `dine_in`
   - `takeout`
   - `delivery`

**Effort:** 1-2 hours (requires careful testing)

---

### 14. Add Foreign Key Constraints
**Status:** 🧹 Tech Debt
**Impact:** Data integrity

**Missing FKs:**
```sql
-- favorites.restaurant_camis should reference restaurants
-- grade_updates.restaurant_camis should reference restaurants (or keep loose for audit)
```

**Note:** May require data cleanup first if orphaned records exist.

**Effort:** 30 minutes

---

## 🔵 P4: FUTURE PHASES

### Phase 3: Core Feature Enrichment

#### 15. Restaurant Detail Dashboard Redesign
**Status:** 📋 Planned
**Goal:** Organize detail screen into tabbed interface

**Current:** Single scrolling view with all data
**Proposed:**
```
RestaurantDetailView
├── Tab 1: "Health Report"
│   ├── Current Grade Card
│   ├── Inspection History
│   └── Violations List
└── Tab 2: "Community Info"
    ├── Google Rating & Reviews
    ├── Opening Hours
    ├── Website Link
    └── Price Level
```

**Files:**
- `RestaurantDetailView.swift` - Major refactor
- New: `HealthReportTabView.swift`
- New: `CommunityInfoTabView.swift`

**Effort:** 1-2 days

---

### Phase 4: Retention & Monetization

#### 17. Favorite Alerts (Push Notifications)
**Status:** 📋 Planned
**Goal:** Notify users when favorite restaurants get new grades

**Backend:**
1. Set up APNs integration (or use Firebase Cloud Messaging)
2. Store device tokens in new `user_devices` table
3. Trigger notifications from `update_database.py` when favorites change
4. Create `/devices` endpoint for token registration

**Frontend:**
1. Request notification permissions
2. Register device token on sign-in
3. Handle incoming notifications
4. Deep link to restaurant detail

**Effort:** 3-5 days

---

#### 18. OpenTable Affiliate Integration
**Status:** 📋 Planned
**Goal:** Revenue from reservation bookings

**Implementation:**
1. Apply for OpenTable affiliate program
2. Add "Reserve Table" button to restaurants with OpenTable
3. Use affiliate link structure
4. Track conversions

**Effort:** 1-2 days (once approved)

---

### Long-Term Roadmap

#### 19. Enhanced Full-Screen Interactive Map
**Status:** 🔮 Future
**Goal:** Browse restaurants on interactive map with clustering

**Features:**
- MapKit with clustering for dense areas
- Filter pins by grade
- Tap for quick preview
- "Search this area" button

**Effort:** 1-2 weeks

---

#### 20. Android App Development
**Status:** 🔮 Future
**Goal:** Reach Android users

**Options:**
1. Native Kotlin app (best performance)
2. React Native / Flutter (code sharing)
3. Kotlin Multiplatform (share logic)

**Effort:** 2-3 months

---

## Summary: Current Priorities

### Immediate (This Week)
| # | Task | Priority | Effort | Type |
|---|------|----------|--------|------|
| 4 | Add database indexes | 🟠 P1 | 15 min | Backend SQL |
| 7 | Token refresh mechanism | 🟠 P1 | 2-4 hrs | iOS |

### Short-Term (Next 2 Weeks)
| # | Task | Priority | Effort | Type |
|---|------|----------|--------|------|
| 11 | N+1 query fix | 🟡 P2 | 30 min | Backend |
| 12 | Frontend defensive sorting | 🟢 P3 | 15 min | iOS |
| 10 | Deep linking | 🟡 P2 | 4-6 hrs | Both |

### Medium-Term (This Month)
| # | Task | Priority | Effort | Type |
|---|------|----------|--------|------|
| 8 | Offline mode / Core Data | 🟡 P2 | 1-2 days | iOS |
| 13 | Schema cleanup | 🟢 P3 | 1-2 hrs | Backend |

### Future (Next Quarter)
| # | Task | Priority | Effort | Type |
|---|------|----------|--------|------|
| 15 | Detail dashboard redesign | 🔵 P4 | 1-2 days | iOS |
| 17 | Push notifications | 🔵 P4 | 3-5 days | Both |
| 18 | OpenTable affiliate | 🔵 P4 | 1-2 days | iOS |

---

## Database Indexes to Run Now

Copy and run in Railway PostgreSQL console:

```sql
-- Run these immediately for 3-10x performance improvement

CREATE INDEX IF NOT EXISTS idx_violations_camis_inspection_date
ON violations (camis, inspection_date);

CREATE INDEX IF NOT EXISTS idx_restaurants_action
ON restaurants (action);

CREATE INDEX IF NOT EXISTS idx_grade_updates_dates
ON grade_updates (update_date DESC, inspection_date DESC);

CREATE INDEX IF NOT EXISTS idx_violations_inspection_date
ON violations (inspection_date);

CREATE INDEX IF NOT EXISTS idx_restaurants_camis_inspection_date_desc
ON restaurants (camis, inspection_date DESC);
```

---

## Files Quick Reference

### Backend Critical Files (this repo)
| File | Purpose |
|------|---------|
| `app_search.py` | All API endpoints (14 total) |
| `update_database.py` | Daily NYC data sync |
| `backfill.py` | Weekly Foursquare/Google matching |
| `enrich_google_data.py` | Google Places enrichment |
| `scripts/schema.sql` | Database schema |
| `config.py` | Environment configuration |

### iOS Critical Files (`nunneryl/CleanPlate-IOS`)
| File | Purpose |
|------|---------|
| `Services/APIService.swift` | Networking, SSL pinning, retry logic |
| `Services/AuthenticationManager.swift` | Apple Sign-In, token storage |
| `Services/MapServices.swift` | Map integration with verification |
| `Features/Search/SearchViewModel.swift` | Search, filters, pagination |
| `Features/RecentlyGraded/RecentlyGradedListViewModel.swift` | Grade updates feed |
| `Features/RestaurantDetail/RestaurantDetailView.swift` | Detail screen |
| `Models/Models.swift` | Data models (Restaurant, Inspection, etc.) |
| `Models/FilterOptions.swift` | Filter enums (Grade, Boro, Cuisine, Sort) |

---

## Architecture Overview

### Backend Stack
- **Framework:** Flask + Gunicorn
- **Database:** PostgreSQL (Railway)
- **Cache:** Redis (1-hour TTL)
- **Auth:** Apple Sign-In JWT verification
- **Cron:** GitHub Actions (daily NYC sync, weekly backfill)

### iOS Stack
- **UI:** SwiftUI
- **Architecture:** MVVM
- **Auth:** Apple Sign-In with Keychain storage
- **Networking:** URLSession with SSL pinning
- **Analytics:** Firebase
- **Security:** Certificate pinning, input validation

### Data Flow
```
NYC Open Data API → Daily Sync → PostgreSQL
                                    ↓
Foursquare/Google → Weekly Backfill → Enrichment Data
                                    ↓
                              Flask API
                                    ↓
                           iOS App (SwiftUI)
```
