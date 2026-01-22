-- =============================================================================
-- WakeCapDW Migration - Unity Catalog Row Filters
-- =============================================================================
-- Converted from SQL Server Row-Level Security (RLS) predicate functions.
-- These functions implement data access control in Unity Catalog.
--
-- Source functions:
--   - security.fn_OrganizationPredicate
--   - security.fn_ProjectPredicate
--   - security.fn_ProjectPredicateEx
--   - security.fn_UserPredicate
--
-- Target: Unity Catalog row filter functions and policies
-- =============================================================================

-- =============================================================================
-- STEP 1: Create Security Schema and Permission Tables
-- =============================================================================

-- Create security schema if not exists
CREATE SCHEMA IF NOT EXISTS wakecap_prod.security;

-- -----------------------------------------------------------------------------
-- User Organization Access Table
-- Maps users to organizations they can access
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wakecap_prod.security.user_organization_access (
    user_email STRING NOT NULL COMMENT 'User email/principal',
    organization_id INT COMMENT 'Organization ID (NULL = all organizations)',
    access_level STRING NOT NULL DEFAULT 'READ' COMMENT 'Access level: READ, WRITE, ADMIN',
    valid_from TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Access start time',
    valid_to TIMESTAMP COMMENT 'Access end time (NULL = no expiry)',
    granted_by STRING COMMENT 'User who granted this access',
    granted_at TIMESTAMP DEFAULT current_timestamp() COMMENT 'When access was granted',
    notes STRING COMMENT 'Additional notes about the access grant',
    CONSTRAINT pk_user_org_access PRIMARY KEY (user_email, organization_id)
) COMMENT 'User access permissions for organizations. Converted from SQL Server RLS.';

-- -----------------------------------------------------------------------------
-- User Project Access Table
-- Maps users to projects they can access
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wakecap_prod.security.user_project_access (
    user_email STRING NOT NULL COMMENT 'User email/principal',
    project_id INT COMMENT 'Project ID (NULL = all projects)',
    access_level STRING NOT NULL DEFAULT 'READ' COMMENT 'Access level: READ, WRITE, ADMIN',
    valid_from TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Access start time',
    valid_to TIMESTAMP COMMENT 'Access end time (NULL = no expiry)',
    granted_by STRING COMMENT 'User who granted this access',
    granted_at TIMESTAMP DEFAULT current_timestamp() COMMENT 'When access was granted',
    notes STRING COMMENT 'Additional notes about the access grant',
    CONSTRAINT pk_user_project_access PRIMARY KEY (user_email, project_id)
) COMMENT 'User access permissions for projects. Converted from SQL Server RLS.';

-- -----------------------------------------------------------------------------
-- User Company Access Table
-- Maps users to companies they can access
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wakecap_prod.security.user_company_access (
    user_email STRING NOT NULL COMMENT 'User email/principal',
    company_id INT COMMENT 'Company ID (NULL = all companies)',
    access_level STRING NOT NULL DEFAULT 'READ' COMMENT 'Access level: READ, WRITE, ADMIN',
    valid_from TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'Access start time',
    valid_to TIMESTAMP COMMENT 'Access end time (NULL = no expiry)',
    CONSTRAINT pk_user_company_access PRIMARY KEY (user_email, company_id)
) COMMENT 'User access permissions for companies.';

-- =============================================================================
-- STEP 2: Create Row Filter Functions
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Organization Filter Function
-- Converted from: security.fn_OrganizationPredicate
-- Returns TRUE if user has access to the given organization
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.security.organization_filter(org_id INT)
RETURNS BOOLEAN
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Row filter for organization access. Converted from security.fn_OrganizationPredicate'
RETURN (
    -- Allow if user has explicit access to this organization
    EXISTS (
        SELECT 1 FROM wakecap_prod.security.user_organization_access
        WHERE user_email = current_user()
        AND (organization_id = org_id OR organization_id IS NULL)  -- NULL = all orgs
        AND valid_from <= current_timestamp()
        AND (valid_to IS NULL OR valid_to > current_timestamp())
    )
);

-- -----------------------------------------------------------------------------
-- Project Filter Function
-- Converted from: security.fn_ProjectPredicate
-- Returns TRUE if user has access to the given project
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.security.project_filter(project_id INT)
RETURNS BOOLEAN
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Row filter for project access. Converted from security.fn_ProjectPredicate'
RETURN (
    -- Allow if user has explicit access to this project
    EXISTS (
        SELECT 1 FROM wakecap_prod.security.user_project_access
        WHERE user_email = current_user()
        AND (project_id = project_filter.project_id OR project_id IS NULL)  -- NULL = all projects
        AND valid_from <= current_timestamp()
        AND (valid_to IS NULL OR valid_to > current_timestamp())
    )
);

-- -----------------------------------------------------------------------------
-- Extended Project Filter Function
-- Converted from: security.fn_ProjectPredicateEx
-- Includes organization-level access inheritance
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.security.project_filter_extended(project_id INT)
RETURNS BOOLEAN
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Extended row filter for project access with org inheritance. Converted from security.fn_ProjectPredicateEx'
RETURN (
    -- Direct project access
    EXISTS (
        SELECT 1 FROM wakecap_prod.security.user_project_access
        WHERE user_email = current_user()
        AND (project_id = project_filter_extended.project_id OR project_id IS NULL)
        AND valid_from <= current_timestamp()
        AND (valid_to IS NULL OR valid_to > current_timestamp())
    )
    OR
    -- Inherited from organization access
    EXISTS (
        SELECT 1 FROM wakecap_prod.security.user_organization_access uoa
        JOIN wakecap_prod.migration.bronze_dbo_Project p ON p.OrganizationID = uoa.organization_id
        WHERE uoa.user_email = current_user()
        AND p.ProjectID = project_filter_extended.project_id
        AND uoa.valid_from <= current_timestamp()
        AND (uoa.valid_to IS NULL OR uoa.valid_to > current_timestamp())
    )
);

-- -----------------------------------------------------------------------------
-- Company Filter Function
-- Returns TRUE if user has access to the given company
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.security.company_filter(company_id INT)
RETURNS BOOLEAN
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Row filter for company access'
RETURN (
    EXISTS (
        SELECT 1 FROM wakecap_prod.security.user_company_access
        WHERE user_email = current_user()
        AND (company_id = company_filter.company_id OR company_id IS NULL)
        AND valid_from <= current_timestamp()
        AND (valid_to IS NULL OR valid_to > current_timestamp())
    )
);

-- -----------------------------------------------------------------------------
-- Admin Check Function
-- Returns TRUE if user is an admin (has access to all data)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION wakecap_prod.security.is_admin()
RETURNS BOOLEAN
LANGUAGE SQL
DETERMINISTIC
COMMENT 'Check if current user is an admin with full access'
RETURN (
    EXISTS (
        SELECT 1 FROM wakecap_prod.security.user_organization_access
        WHERE user_email = current_user()
        AND organization_id IS NULL  -- NULL = all organizations
        AND access_level = 'ADMIN'
        AND valid_from <= current_timestamp()
        AND (valid_to IS NULL OR valid_to > current_timestamp())
    )
);

-- =============================================================================
-- STEP 3: Apply Row Filters to Tables
-- =============================================================================
-- Note: Execute these statements to enable row-level security on tables.
-- Only run after the tables have been created and populated.

-- Workers table - filter by project
-- ALTER TABLE wakecap_prod.migration.silver_worker
-- SET ROW FILTER wakecap_prod.security.project_filter ON (ProjectID);

-- Projects table - filter by project
-- ALTER TABLE wakecap_prod.migration.silver_project
-- SET ROW FILTER wakecap_prod.security.project_filter ON (ProjectID);

-- Crews table - filter by project
-- ALTER TABLE wakecap_prod.migration.silver_crew
-- SET ROW FILTER wakecap_prod.security.project_filter ON (ProjectID);

-- Fact Workers Shifts - filter by project
-- ALTER TABLE wakecap_prod.migration.silver_workers_shifts
-- SET ROW FILTER wakecap_prod.security.project_filter ON (ProjectID);

-- Fact Observations - filter by project
-- ALTER TABLE wakecap_prod.migration.silver_fact_observations
-- SET ROW FILTER wakecap_prod.security.project_filter ON (ProjectID);

-- Organizations table - filter by organization
-- ALTER TABLE wakecap_prod.migration.silver_organization
-- SET ROW FILTER wakecap_prod.security.organization_filter ON (OrganizationID);

-- =============================================================================
-- STEP 4: Grant Permissions
-- =============================================================================
-- Grant data engineers ability to manage security tables

-- GRANT SELECT, INSERT, UPDATE, DELETE ON wakecap_prod.security.user_organization_access TO `data-engineers`;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON wakecap_prod.security.user_project_access TO `data-engineers`;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON wakecap_prod.security.user_company_access TO `data-engineers`;

-- Grant all users ability to use filter functions
-- GRANT EXECUTE ON FUNCTION wakecap_prod.security.organization_filter TO `account users`;
-- GRANT EXECUTE ON FUNCTION wakecap_prod.security.project_filter TO `account users`;
-- GRANT EXECUTE ON FUNCTION wakecap_prod.security.project_filter_extended TO `account users`;
-- GRANT EXECUTE ON FUNCTION wakecap_prod.security.company_filter TO `account users`;
-- GRANT EXECUTE ON FUNCTION wakecap_prod.security.is_admin TO `account users`;

-- =============================================================================
-- STEP 5: Sample Data for Testing
-- =============================================================================
-- Insert test permissions (uncomment to use)

-- Grant admin access to specific user
-- INSERT INTO wakecap_prod.security.user_organization_access
-- (user_email, organization_id, access_level, granted_by)
-- VALUES ('admin@wakecap.com', NULL, 'ADMIN', 'system');

-- Grant project access to a user
-- INSERT INTO wakecap_prod.security.user_project_access
-- (user_email, project_id, access_level, granted_by)
-- VALUES ('user@wakecap.com', 123, 'READ', 'admin@wakecap.com');

-- Grant organization access to a user
-- INSERT INTO wakecap_prod.security.user_organization_access
-- (user_email, organization_id, access_level, granted_by)
-- VALUES ('manager@wakecap.com', 1, 'WRITE', 'admin@wakecap.com');

-- =============================================================================
-- USAGE EXAMPLES
-- =============================================================================
/*
-- Check if current user has access to project 123
SELECT wakecap_prod.security.project_filter(123);

-- Check if current user is admin
SELECT wakecap_prod.security.is_admin();

-- Query workers with automatic filtering (after row filter is applied)
SELECT * FROM wakecap_prod.migration.silver_worker;
-- Only returns rows where user has access to the ProjectID

-- Manual filtering in query (when row filter not applied)
SELECT * FROM wakecap_prod.migration.bronze_dbo_Worker w
WHERE wakecap_prod.security.project_filter(w.ProjectID);

-- View current user's permissions
SELECT * FROM wakecap_prod.security.user_project_access
WHERE user_email = current_user();

-- View all accessible projects for current user
SELECT DISTINCT p.ProjectID, p.ProjectName
FROM wakecap_prod.migration.bronze_dbo_Project p
WHERE wakecap_prod.security.project_filter_extended(p.ProjectID);
*/

-- =============================================================================
-- MIGRATION NOTES
-- =============================================================================
/*
Key differences from SQL Server RLS:

1. Unity Catalog row filters are more declarative and simpler
2. No need for separate security policies - filters are applied directly to tables
3. Use current_user() instead of SESSION_CONTEXT/USER_NAME()
4. Cannot use APPLY SECURITY POLICY - use ALTER TABLE SET ROW FILTER instead
5. Filter functions must be DETERMINISTIC
6. Performance: Row filters are pushed down to storage layer when possible

Migration steps:
1. Create permission tables
2. Migrate permission data from SQL Server security tables
3. Create row filter functions
4. Test filters manually before applying to tables
5. Apply filters to tables one at a time, testing after each
6. Monitor query performance after enabling filters
*/
