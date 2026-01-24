# Databricks Deployment Report

**Last Updated:** 2026-01-24
**Deployed By:** khaledauf@wakecap.com

## Databricks Connection
- **Host:** https://adb-3022397433351638.18.azuredatabricks.net
- **Catalog:** wakecap_prod

---

## Bronze Layer (DEPLOYED)

**Deployment Time:** 2026-01-22

| Metric | Value |
|--------|-------|
| Job Name | WakeCapDW_Bronze_TimescaleDB_Raw |
| Target Schema | wakecap_prod.raw |
| Tables | 78 (timescale_*) |
| Schedule | Daily 2:00 AM UTC |

### Files Deployed
- **Notebook:** /Workspace/migration_project/pipelines/timescaledb/notebooks/bronze_loader_optimized
- **Config:** /Workspace/migration_project/pipelines/timescaledb/config/timescaledb_tables_v2.yml

---

## Silver Layer (DEPLOYED)

**Deployment Time:** 2026-01-24T00:00:00

| Metric | Value |
|--------|-------|
| Job Name | WakeCapDW_Silver_TimescaleDB |
| Job ID | 181959206191493 |
| Target Schema | wakecap_prod.silver |
| Tables | 77 (silver_*) |
| Tasks | 8 (with dependencies) |
| Schedule | Daily 3:00 AM UTC (Paused) |

### Job Structure

```
silver_independent_dimensions ----+
                                  +--> silver_project_children --> silver_zone_dependent --+--> silver_assignments --> silver_facts
silver_organization --> silver_project --+                                                  |
                                                                                            +--> silver_history
```

### Processing Groups

| Task | Tables | Cluster |
|------|--------|---------|
| silver_independent_dimensions | 11 | 2 workers |
| silver_organization | 2 | 2 workers |
| silver_project | 1 | 2 workers |
| silver_project_children | 16 | 2 workers |
| silver_zone_dependent | 7 | 2 workers |
| silver_assignments | 17 | 2 workers |
| silver_facts | 20 | 4 workers |
| silver_history | 3 | 2 workers |

### Files Deployed
- **Notebook:** /Workspace/migration_project/pipelines/silver/notebooks/silver_loader
- **Config:** /Workspace/migration_project/pipelines/silver/config/silver_tables.yml
- **DDL:** /Workspace/migration_project/pipelines/silver/ddl/create_silver_watermarks.sql

### Link
- [Silver Job](https://adb-3022397433351638.18.azuredatabricks.net/jobs/181959206191493)

---

## DLT Pipeline (Gold Layer)

**Deployment Time:** 2026-01-18T19:05:51.406370

| Metric | Value |
|--------|-------|
| Pipeline Name | WakeCapDW_Migration_Pipeline |
| Pipeline ID | bc59257f-73aa-42ed-9fc6-01a6c14dbb0a |
| Target Schema | wakecap_prod.migration |
| Development Mode | True |
| Tables | 30 |
| Views | 10 |

### Files Deployed
- **Notebook:** /Workspace/Shared/migrations/wakecap/wakecap_migration_pipeline

### Link
- [DLT Pipeline](https://adb-3022397433351638.18.azuredatabricks.net/pipelines/bc59257f-73aa-42ed-9fc6-01a6c14dbb0a)

---

## Schedule Coordination

| Job | Schedule | Status |
|-----|----------|--------|
| WakeCapDW_Bronze_TimescaleDB_Raw | 2:00 AM UTC | Active |
| WakeCapDW_Silver_TimescaleDB | 3:00 AM UTC | Paused |
| Gold Layer (DLT) | TBD | Development |

---

## Next Steps

1. **Run Silver Initial Load:**
   - Go to: https://adb-3022397433351638.18.azuredatabricks.net/jobs/181959206191493
   - Click "Run now"

2. **Enable Silver Schedule:**
   - After successful initial load
   - Change from "Paused" to "Active"

3. **Verify Silver Data:**
   ```sql
   SHOW TABLES IN wakecap_prod.silver;

   SELECT table_name, last_load_status, last_load_row_count
   FROM wakecap_prod.migration._silver_watermarks
   ORDER BY processing_group;
   ```

4. **Deploy Gold Layer:**
   - Configure and start DLT pipeline
   - Schedule for 4:00 AM UTC
