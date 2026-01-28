"""
Verify ADF to Databricks gaps by querying actual Bronze/Silver data.
Run this in Databricks or locally with Databricks SDK.
"""

from databricks.sdk import WorkspaceClient
import os

# Configuration - uses environment variables or credentials file
# Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables, or use ~/.databrickscfg

def get_client():
    """Get Databricks client from environment or credentials file."""
    return WorkspaceClient()


# Verification queries to run in Databricks SQL
VERIFICATION_QUERIES = """
-- ========================================
-- ADF Gap Verification Queries
-- Run these in Databricks SQL to verify gaps
-- ========================================

-- 1. Check if ApprovedById is populated in ResourceTimesheet
SELECT
    'timescale_resourcetimesheet' as table_name,
    COUNT(*) as total_rows,
    COUNT(ApprovedById) as with_approved_by_id,
    COUNT(ApprovedBy) as with_approved_by,
    COUNT(CASE WHEN ApprovedById IS NOT NULL AND ApprovedBy IS NOT NULL THEN 1 END) as both_populated
FROM wakecap_prod.raw.timescale_resourcetimesheet;

-- 2. Check date ranges in key tables for outliers
SELECT 'timescale_crewcomposition' as table_name,
    MIN(EffectiveDate) as min_date,
    MAX(EffectiveDate) as max_date,
    COUNT(CASE WHEN EffectiveDate < '2000-01-01' OR EffectiveDate > '2100-01-01' THEN 1 END) as outliers
FROM wakecap_prod.raw.timescale_crewcomposition
UNION ALL
SELECT 'timescale_workshiftresourceassignment',
    MIN(EffectiveDate), MAX(EffectiveDate),
    COUNT(CASE WHEN EffectiveDate < '2000-01-01' OR EffectiveDate > '2100-01-01' THEN 1 END)
FROM wakecap_prod.raw.timescale_workshiftresourceassignment
UNION ALL
SELECT 'timescale_crewmanager',
    MIN(EffectiveDate), MAX(EffectiveDate),
    COUNT(CASE WHEN EffectiveDate < '2000-01-01' OR EffectiveDate > '2100-01-01' THEN 1 END)
FROM wakecap_prod.raw.timescale_crewmanager
UNION ALL
SELECT 'timescale_obs',
    MIN(EffectiveDate), MAX(EffectiveDate),
    COUNT(CASE WHEN EffectiveDate < '2000-01-01' OR EffectiveDate > '2100-01-01' THEN 1 END)
FROM wakecap_prod.raw.timescale_obs;

-- 3. Check LinkedUserId availability for ApprovedBy resolution
SELECT
    COUNT(*) as total_workers,
    COUNT(LinkedUserId) as with_linked_user_id,
    COUNT(DISTINCT LinkedUserId) as unique_linked_user_ids
FROM wakecap_prod.raw.timescale_people
WHERE DeletedAt IS NULL;

-- 4. Check if ApprovedBy values exist in People.LinkedUserId
SELECT
    rt_count.total_with_approved_by,
    matched.matched_to_worker,
    rt_count.total_with_approved_by - COALESCE(matched.matched_to_worker, 0) as unmatched
FROM
    (SELECT COUNT(DISTINCT ApprovedBy) as total_with_approved_by
     FROM wakecap_prod.raw.timescale_resourcetimesheet
     WHERE ApprovedBy IS NOT NULL) rt_count
CROSS JOIN
    (SELECT COUNT(DISTINCT rt.ApprovedBy) as matched_to_worker
     FROM wakecap_prod.raw.timescale_resourcetimesheet rt
     INNER JOIN wakecap_prod.raw.timescale_people p ON rt.ApprovedBy = p.LinkedUserId
     WHERE rt.ApprovedBy IS NOT NULL) matched;

-- 5. Check DeviceLocation geometry data
SELECT
    'timescale_devicelocation' as table_name,
    COUNT(*) as total_rows,
    COUNT(PointWKT) as with_geometry,
    COUNT(SpaceId) as with_space_id
FROM wakecap_prod.raw.timescale_devicelocation
LIMIT 1;

-- 6. Check Zone geometry data
SELECT
    'timescale_zone' as table_name,
    COUNT(*) as total_rows,
    COUNT(CoordinatesWKT) as with_geometry,
    COUNT(SpaceId) as with_space_id
FROM wakecap_prod.raw.timescale_zone;

-- 7. Check Worker date columns for outliers
SELECT
    COUNT(*) as total,
    COUNT(CASE WHEN JoinDate < '2000-01-01' OR JoinDate > '2100-01-01' THEN 1 END) as join_date_outliers,
    COUNT(CASE WHEN ReleaseDate < '2000-01-01' OR ReleaseDate > '2100-01-01' THEN 1 END) as release_date_outliers,
    COUNT(CASE WHEN ActivatedAt < '2000-01-01' OR ActivatedAt > '2100-01-01' THEN 1 END) as activated_outliers,
    COUNT(CASE WHEN DeActivatedAt < '2000-01-01' OR DeActivatedAt > '2100-01-01' THEN 1 END) as deactivated_outliers
FROM wakecap_prod.raw.timescale_people;

-- 8. Check MV_ResourceDevice_NoViolation equivalent
-- NOTE: The PostgreSQL MV uses window function to detect overlapping assignments, NOT DeletedAt filtering
-- The MV filters rows where previous UnAssignedAt > current AssignedAt (violation)
-- ResourceDevice table does NOT have DeletedAt column
SELECT
    DeviceId,
    ResourceId,
    AssignedAt,
    UnAssignedAt
FROM wakecap_prod.raw.timescale_resourcedevice
LIMIT 10;

-- 9. Compare Gold fact_reported_attendance dimensions
-- Check if all ApprovedById values resolve correctly
SELECT
    COUNT(*) as total_attendance,
    COUNT(ApprovedById) as with_approved_by,
    COUNT(dim_ApprovedByWorkerID) as resolved_to_worker
FROM wakecap_prod.gold.gold_fact_reported_attendance;
"""

def print_queries():
    print("=" * 60)
    print("ADF Gap Verification Queries")
    print("=" * 60)
    print("\nCopy and run these queries in Databricks SQL Warehouse:\n")
    print(VERIFICATION_QUERIES)


if __name__ == "__main__":
    print_queries()
