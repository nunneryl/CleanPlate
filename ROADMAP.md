# CleanPlate Development Roadmap

> **Last Updated:** February 10, 2026
> **Status:** Active Development

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

## 🔴 P0: CRITICAL BUG FIXES

### 1. ~~Account Deletion Cache Bug~~
**Status:** ✅ Completed
**Impact:** Users see old favorites after deleting and recreating account
**Root Cause:** Redis cache not cleared on account deletion — fixed by adding `cache.delete()` after account deletion.

---

### 2. ~~Grade Updates Sorting Bug~~
**Status:** ✅ Completed
**Impact:** Recently graded restaurants appear in wrong order; some missed entirely
**Root Cause:** Sorting by `update_date` instead of `grade_date` — fixed with `COALESCE` ordering and expanded date filter.

---

### 3. ~~Remove "Grade Pending" from Recently Graded Filters~~
**Status:** ✅ Completed
**Impact:** Confusing filter option — removed "Pending" from grade filter in Recently Graded view.

---

## 🟠 P1: HIGH PRIORITY - Performance & Reliability

### 4. ~~Missing Database Indexes (Critical Performance)~~
**Status:** ✅ Completed (February 2026)
**Impact:** 3-10x slower queries on main endpoints

Added indexes for cuisine, boro, location, and grade lookups in Railway PostgreSQL. Removed unused indexes to save ~30MB.

---

### 5. ~~Fix CAST Preventing Index Usage~~
**Status:** ✅ Completed (previously fixed)
**Impact:** `CAST()` was preventing index usage — already updated to `::date` cast in `update_database.py`.

---

### 6. Map Integration Reliability
**Status:** ⚠️ Reliability Issue
**Impact:** Silent failures when Apple Maps can't find restaurant; no fallback

**Issues Identified:**
- No fallback cascade between Apple Maps → Google Maps
- Silent failures with no user feedback
- Google Maps button hidden if `google_place_id` is null
- No coordinate verification

**Files to Update:**
- `MapServices.swift` - Add fallback search strategies
- `RestaurantDetailViewModel.swift` - Add dual-source search
- `RestaurantDetailView.swift` - Show both map options when available

**Recommended Approach:**
1. Search Apple Maps AND check for Google Place ID in parallel
2. If Apple fails but Google ID exists, offer Google as alternative
3. Show clear error messages with recovery options
4. Verify returned coordinates are within ~1000ft of API coordinates

**Effort:** 2-3 hours

---

### 7. ~~Apple Token Refresh Mechanism~~
**Status:** ✅ Completed (February 2026)
**Impact:** Users no longer get logged out when Apple identity token expires

**Implemented:**
- Silent token refresh via `ASAuthorizationController` in `AuthenticationManager.swift`
- 401 interception in `APIService.swift` with automatic retry using refreshed token
- Concurrent refresh coalescing (multiple 401s trigger only one refresh)
- Proactive credential state check on app foreground in `NYCFoodRatingsApp.swift`
- Graceful fallback: if refresh fails, error surfaces normally (no infinite loop)

---

## 🟡 P2: MEDIUM PRIORITY - User Experience

### 8. Offline Mode / Local Database
**Status:** 🆕 New Feature
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

### 9. Error Recovery & Retry Logic
**Status:** 🆕 Enhancement
**Impact:** Better UX when network is flaky

**Current:** Basic retry on 5xx errors
**Needed:**
- Granular retry for different error types
- User-initiated retry buttons
- Exponential backoff with jitter
- Offline queue for favorites/searches

**Files:**
- `APIService.swift` - Enhanced retry logic
- All ViewModels - Add retry actions

**Effort:** 4-6 hours

---

### 10. Deep Linking / Universal Links
**Status:** 🆕 New Feature
**Impact:** Can't share restaurant links that open in app

**Implementation:**
1. Configure Apple App Site Association file
2. Add URL handling in `CleanPlateApp.swift`
3. Create shareable URLs: `cleanplate.app/restaurant/{camis}`
4. Handle incoming links and navigate to detail view

**Files:**
- `apple-app-site-association` (new, hosted on backend)
- `CleanPlateApp.swift` - Add `.onOpenURL` handler
- Backend: Add redirect endpoint

**Effort:** 4-6 hours

---

### 11. ~~N+1 Query Fix in Backfill Script~~
**Status:** ✅ Completed (February 2026)
**Impact:** Replaced 50K+ individual queries with a single `LAG()` window function query in `backfill_grade_updates.py`.

---

## 🟢 P3: LOW PRIORITY - Enhancements

### 12. ~~Frontend Defensive Sorting~~
**Status:** ✅ Completed (February 2026)
**Impact:** `RecentlyGradedListViewModel.swift` now sorts by `finalized_date`/`grade_date` client-side as a safety net.

---

### 13. Schema Cleanup - Remove Duplicate Violation Columns
**Status:** 🧹 Tech Debt
**Impact:** Cleaner schema, less confusion

**Issue:** Both `restaurants.violation_code/description` AND `violations` table store violations

**Fix:**
1. Verify all code uses `violations` table
2. Drop redundant columns from `restaurants` table
3. Update any queries still using old columns

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

**Effort:** 30 minutes

---

## 🔵 P4: FUTURE PHASES

---

### Phase 3: Core Feature Enrichment

#### 15. Restaurant Detail Dashboard Redesign
**Status:** 📋 Planned
**Goal:** Organize detail screen into tabbed interface

**New Structure:**
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

#### 16. Google Maps Data Integration (Full)
**Status:** 📋 Planned
**Goal:** Enrich app with Google ratings, hours, website

**Backend Tasks:**
1. ✅ Schema columns already exist (`google_rating`, `hours`, `website`, etc.)
2. Run Apify Google Maps Scraper for full dataset
3. Import data via `import_apify_data.py`
4. Update `/restaurant/<camis>` endpoint to return Google data

**Frontend Tasks:**
1. Update `Restaurant` model if needed
2. Build "Community Info" tab UI
3. Display ratings with star icons
4. Format hours in user-friendly way
5. Add "View on Google Maps" deep link

**Effort:** 2-3 days total

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

#### 21. Account Creation Landing Page
**Status:** 📋 Planned
**Goal:** Better onboarding when unauthenticated users try account-only features (e.g. favorites)
**Current:** Basic sign-in prompt
**Improvement:** Design a landing page with reasons to create an account (save favorites, track searches, etc.)

**Effort:** 2-4 hours

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

## Summary: Implementation Order

### Week 1: Critical Fixes
| # | Task | Priority | Effort |
|---|------|----------|--------|
| 1 | ~~Account deletion cache bug~~ | 🔴 P0 | ✅ Done |
| 2 | ~~Grade updates sorting bug~~ | 🔴 P0 | ✅ Done |
| 3 | ~~Remove pending from filters~~ | 🔴 P0 | ✅ Done |
| 4 | ~~Add database indexes~~ | 🟠 P1 | ✅ Done |
| 5 | ~~Fix CAST in update script~~ | 🟠 P1 | ✅ Done |

### Week 2: Reliability
| # | Task | Priority | Effort |
|---|------|----------|--------|
| 4 | ~~Add database indexes~~ | 🟠 P1 | ✅ Done |
| 7 | ~~Token refresh mechanism~~ | 🟠 P1 | ✅ Done |
| 6 | Map integration reliability | 🟠 P1 | 2-3 hrs |
| 11 | ~~N+1 query fix~~ | 🟡 P2 | ✅ Done |
| 12 | ~~Frontend defensive sorting~~ | 🟢 P3 | ✅ Done |

### Week 3-4: User Experience
| # | Task | Priority | Effort |
|---|------|----------|--------|
| 8 | Offline mode / Core Data | 🟡 P2 | 1-2 days |
| 9 | Error recovery / retry | 🟡 P2 | 4-6 hrs |
| 10 | Deep linking | 🟡 P2 | 4-6 hrs |

### Month 2: Feature Enrichment
| # | Task | Priority | Effort |
|---|------|----------|--------|
| 15 | Detail dashboard redesign | 🔵 P4 | 1-2 days |
| 16 | Google Maps data integration | 🔵 P4 | 2-3 days |

### Month 3+: Retention & Growth
| # | Task | Priority | Effort |
|---|------|----------|--------|
| 17 | Push notifications | 🔵 P4 | 3-5 days |
| 18 | OpenTable affiliate | 🔵 P4 | 1-2 days |
| 19 | Interactive map | 🔮 Future | 1-2 weeks |
| 20 | Android app | 🔮 Future | 2-3 months |

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

### Backend Critical Files
| File | Lines | Key Functions |
|------|-------|---------------|
| `app_search.py` | 763 | All API endpoints |
| `update_database.py` | 246 | NYC data sync |
| `scripts/schema.sql` | - | Database schema |

### iOS Critical Files
| File | Lines | Key Functions |
|------|-------|---------------|
| `APIService.swift` | 368 | Networking |
| `AuthenticationManager.swift` | 216 | Auth + state |
| `SearchViewModel.swift` | 232 | Search logic |
| `RecentlyGradedListViewModel.swift` | 72 | Grade updates |
| `RestaurantDetailView.swift` | - | Detail screen |
| `MapServices.swift` | - | Map integration |
