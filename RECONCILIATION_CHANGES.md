# Settlement Reconciliation Enhancement - Summary of Changes

## Overview
Enhanced the settlement reconciliation system with sophisticated multi-stage matching logic and new database tracking columns.

## Python Code Changes (function_app.py)

### New Functions Added

1. **`load_strict_deals_to_process(conn)`**
   - Loads internal deals with strict filters (reason=0, status NOT IN (4,7), etc.)
   - Used for primary match attempts

2. **`load_broad_trade_search(conn)`**
   - Loads all internal deals with minimal filters for fallback searches
   - Used when exact matches aren't found

3. **`exact_score(st, td) -> (score, notes)`**
   - Scores a trade pair on: value_date (20pts), quantity (30pts), price (20pts), amount (20pts)
   - Returns score and list of mismatches

4. **`find_strict_candidates(st, deals) -> List[Dict]`**
   - Filters candidates by ISIN + Side + Trade Date
   - Primary matching criteria

5. **`try_exact_single_match(st, candidates) -> (match, status, note)`**
   - Finds single best match from candidates
   - Returns: "MATCHED" (>=90pts), "PARTIAL" (>0pts), or None

6. **`try_aggregate_match(st, candidates) -> (rows, status, note)`**
   - Matches external trade against multiple internal trades summed by settlement date
   - For cases where 1 external = N internal trades
   - Returns: "MATCHED_AGGREGATED" or None

7. **`find_similar_broad_rows(st, broad_deals) -> List[Dict]`**
   - Finds similar ISINs outside strict match criteria
   - Fallback for unmatched trades
   - Limits to 10 results

### Updated Functions

1. **`upsert_reconciliation_result()`**
   - Added parameters:
     - `matched_internal_ids: Optional[str]` - Comma-separated IDs of matched internal trades
     - `matched_internal_count: Optional[int]` - Number of matched internal records
     - `mismatch_json: Optional[str]` - JSON with detailed field mismatches (PARTIAL matches)
     - `external_price: numeric` - External trade price for tracking
     - `internal_price: numeric` - Internal trade price for tracking
     - `external_value_date: date` - External settlement date
     - `internal_value_date: date` - Internal settlement date

2. **`run_settlement_reconciliation(conn, run_id)`**
   - New logic: Multi-stage matching (Exact → Aggregated → Similar → Not Found)
   - Tracks: matched_count, matched_aggregated_count, partial_count, not_found_count, similar_found_count
   - For each external trade:
     1. Try exact single match (90+ points = MATCHED, >0 points = PARTIAL)
     2. Try aggregate match (multiple internal rows summing to external qty/amount)
     3. Try similar ISIN search in broad deals
     4. Mark as NOT_FOUND if all fail
   - Populates new database columns with detailed tracking

## Database Changes

### New Columns Added to `settlement_reconciliation` Table

```sql
ALTER TABLE back_office_auto.settlement_reconciliation
    ADD COLUMN IF NOT EXISTS matched_internal_ids text,
    ADD COLUMN IF NOT EXISTS matched_internal_count integer,
    ADD COLUMN IF NOT EXISTS mismatch_json jsonb,
    ADD COLUMN IF NOT EXISTS external_price numeric,
    ADD COLUMN IF NOT EXISTS internal_price numeric,
    ADD COLUMN IF NOT EXISTS external_value_date date,
    ADD COLUMN IF NOT EXISTS internal_value_date date;
```

## Migration Instructions

### Step 1: Run Database Migration
```bash
export PG_CONN_STRING="your_connection_string"
python migrate_add_reconciliation_columns.py
```

Expected output:
```
Connecting to PostgreSQL...
SUCCESS: All columns added to settlement_reconciliation table
```

### Step 2: Deploy Function App
- Push updated `function_app.py` to your Azure Functions deployment

### Step 3: Verify Operation
- Monitor Azure Function logs for reconciliation runs
- Check `settlement_reconciliation` table for new column values
- Verify `matched_aggregated_count` and `similar_found_count` in agent run logs

## Matching Algorithm Details

### Match Status Values
- **MATCHED**: Single internal trade matches exactly (90+ points)
- **PARTIAL**: Single internal trade matches partially (>0 points, <90)
- **MATCHED_AGGREGATED**: Multiple internal trades collectively match (sum = external)
- **SIMILAR_FOUND_OUTSIDE_STRICT_SCOPE**: Same ISIN found but outside strict filters
- **NOT_FOUND**: No matches found by any method

### Scoring Formula (Single Match)
| Criterion | Points | Condition |
|-----------|--------|-----------|
| Value Date | 20 | External value_date == internal settle_date |
| Quantity | 30 | External qty ≈ internal qty (tolerance: 0.0001) |
| Price | 20 | External price ≈ internal price (tolerance: 0.0001) |
| Amount | 20 | External consideration ≈ internal transaction_value (tolerance: 0.01) |
| **TOTAL** | **90** | Threshold for "MATCHED" status |

## Backward Compatibility
- All existing functions preserved
- `find_best_match_for_trade()` kept for reference
- New functions call existing helpers (clean_text, parse_decimal, values_equal_decimal)
- No changes to email parsing pipeline

## Testing Recommendations
1. Run against test data with known matches
2. Verify aggregated match logic with multi-position trades
3. Check similar ISIN fallback with edge cases
4. Monitor performance with large result sets (mismatch_json serialization)
