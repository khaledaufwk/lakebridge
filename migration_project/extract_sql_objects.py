#!/usr/bin/env python3
"""
Extract SQL objects (stored procedures, views, functions, tables) from SQL Server.
"""
import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from databricks.labs.lakebridge.connections.database_manager import DatabaseManager


def load_credentials():
    """Load credentials from the lakebridge credentials file."""
    creds_path = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"
    with open(creds_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def sanitize_filename(name):
    """Sanitize a name to be a valid filename."""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, '_')
    return name


def extract_stored_procedures(db_manager, output_dir):
    """Extract all stored procedures."""
    print("\n--- Extracting Stored Procedures ---")
    
    query = """
        SELECT 
            s.name AS schema_name,
            p.name AS proc_name,
            OBJECT_DEFINITION(p.object_id) AS definition
        FROM sys.procedures p
        JOIN sys.schemas s ON p.schema_id = s.schema_id
        WHERE OBJECT_DEFINITION(p.object_id) IS NOT NULL
        ORDER BY s.name, p.name
    """
    
    result = db_manager.fetch(query)
    count = 0
    
    for row in result.rows:
        schema_name, proc_name, definition = row
        if definition:
            filename = sanitize_filename(f"{schema_name}.{proc_name}.sql")
            filepath = output_dir / "stored_procedures" / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"-- Schema: {schema_name}\n")
                f.write(f"-- Procedure: {proc_name}\n")
                f.write(f"-- Extracted from WakeCapDW\n\n")
                f.write(definition)
            count += 1
            print(f"  [OK] {schema_name}.{proc_name}")
    
    print(f"  Total stored procedures extracted: {count}")
    return count


def extract_views(db_manager, output_dir):
    """Extract all views."""
    print("\n--- Extracting Views ---")
    
    query = """
        SELECT 
            s.name AS schema_name,
            v.name AS view_name,
            OBJECT_DEFINITION(v.object_id) AS definition
        FROM sys.views v
        JOIN sys.schemas s ON v.schema_id = s.schema_id
        WHERE OBJECT_DEFINITION(v.object_id) IS NOT NULL
        ORDER BY s.name, v.name
    """
    
    result = db_manager.fetch(query)
    count = 0
    
    for row in result.rows:
        schema_name, view_name, definition = row
        if definition:
            filename = sanitize_filename(f"{schema_name}.{view_name}.sql")
            filepath = output_dir / "views" / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"-- Schema: {schema_name}\n")
                f.write(f"-- View: {view_name}\n")
                f.write(f"-- Extracted from WakeCapDW\n\n")
                f.write(definition)
            count += 1
            print(f"  [OK] {schema_name}.{view_name}")
    
    print(f"  Total views extracted: {count}")
    return count


def extract_functions(db_manager, output_dir):
    """Extract all user-defined functions."""
    print("\n--- Extracting Functions ---")
    
    query = """
        SELECT 
            s.name AS schema_name,
            o.name AS func_name,
            o.type_desc AS func_type,
            OBJECT_DEFINITION(o.object_id) AS definition
        FROM sys.objects o
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type IN ('FN', 'IF', 'TF', 'AF')  -- Scalar, Inline Table, Table-valued, Aggregate
        AND OBJECT_DEFINITION(o.object_id) IS NOT NULL
        ORDER BY s.name, o.name
    """
    
    result = db_manager.fetch(query)
    count = 0
    
    # Create functions directory
    func_dir = output_dir / "functions"
    func_dir.mkdir(exist_ok=True)
    
    for row in result.rows:
        schema_name, func_name, func_type, definition = row
        if definition:
            filename = sanitize_filename(f"{schema_name}.{func_name}.sql")
            filepath = func_dir / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"-- Schema: {schema_name}\n")
                f.write(f"-- Function: {func_name}\n")
                f.write(f"-- Type: {func_type}\n")
                f.write(f"-- Extracted from WakeCapDW\n\n")
                f.write(definition)
            count += 1
            print(f"  [OK] {schema_name}.{func_name} ({func_type})")
    
    print(f"  Total functions extracted: {count}")
    return count


def extract_table_schemas(db_manager, output_dir):
    """Extract CREATE TABLE statements for all tables."""
    print("\n--- Extracting Table Definitions ---")
    
    # First get all tables
    tables_query = """
        SELECT 
            s.name AS schema_name,
            t.name AS table_name
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        ORDER BY s.name, t.name
    """
    
    tables_result = db_manager.fetch(tables_query)
    count = 0
    
    for row in tables_result.rows:
        schema_name, table_name = row
        
        # Get column definitions
        columns_query = f"""
            SELECT 
                c.name AS column_name,
                TYPE_NAME(c.user_type_id) AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                OBJECT_DEFINITION(c.default_object_id) AS default_value
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID('[{schema_name}].[{table_name}]')
            ORDER BY c.column_id
        """
        
        columns_result = db_manager.fetch(columns_query)
        
        # Build CREATE TABLE statement
        create_stmt = f"-- Schema: {schema_name}\n"
        create_stmt += f"-- Table: {table_name}\n"
        create_stmt += f"-- Extracted from WakeCapDW\n\n"
        create_stmt += f"CREATE TABLE [{schema_name}].[{table_name}] (\n"
        
        column_defs = []
        for col in columns_result.rows:
            col_name, data_type, max_len, precision, scale, is_nullable, is_identity, default_val = col
            
            # Build data type string
            if data_type in ('varchar', 'nvarchar', 'char', 'nchar', 'varbinary', 'binary'):
                if max_len == -1:
                    type_str = f"{data_type}(MAX)"
                else:
                    type_str = f"{data_type}({max_len})"
            elif data_type in ('decimal', 'numeric'):
                type_str = f"{data_type}({precision},{scale})"
            elif data_type == 'datetime2':
                type_str = f"{data_type}({scale})"
            else:
                type_str = data_type
            
            # Build column definition
            col_def = f"    [{col_name}] {type_str}"
            
            if is_identity:
                col_def += " IDENTITY(1,1)"
            
            if not is_nullable:
                col_def += " NOT NULL"
            else:
                col_def += " NULL"
            
            if default_val:
                col_def += f" DEFAULT {default_val}"
            
            column_defs.append(col_def)
        
        create_stmt += ",\n".join(column_defs)
        create_stmt += "\n);\n"
        
        # Get primary key
        pk_query = f"""
            SELECT c.name
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE i.object_id = OBJECT_ID('[{schema_name}].[{table_name}]')
            AND i.is_primary_key = 1
            ORDER BY ic.key_ordinal
        """
        
        pk_result = db_manager.fetch(pk_query)
        pk_columns = [r[0] for r in pk_result.rows]
        
        if pk_columns:
            pk_cols_str = ", ".join([f"[{c}]" for c in pk_columns])
            create_stmt += f"\n-- Primary Key: ({pk_cols_str})\n"
        
        filename = sanitize_filename(f"{schema_name}.{table_name}.sql")
        filepath = output_dir / "tables" / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(create_stmt)
        
        count += 1
        print(f"  [OK] {schema_name}.{table_name}")
    
    print(f"  Total tables extracted: {count}")
    return count


def generate_summary(output_dir, counts):
    """Generate a summary of extracted objects."""
    summary_path = output_dir / "EXTRACTION_SUMMARY.md"
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("# SQL Object Extraction Summary\n\n")
        f.write(f"**Source Database:** WakeCapDW_20251215\n")
        f.write(f"**Server:** wakecap24.database.windows.net\n\n")
        f.write("## Extracted Objects\n\n")
        f.write("| Object Type | Count |\n")
        f.write("|-------------|-------|\n")
        f.write(f"| Stored Procedures | {counts['procedures']} |\n")
        f.write(f"| Views | {counts['views']} |\n")
        f.write(f"| Functions | {counts['functions']} |\n")
        f.write(f"| Tables | {counts['tables']} |\n")
        f.write(f"| **Total** | **{sum(counts.values())}** |\n\n")
        f.write("## Directory Structure\n\n")
        f.write("```\n")
        f.write("source_sql/\n")
        f.write("  stored_procedures/  # SQL Server stored procedures\n")
        f.write("  views/              # Database views\n")
        f.write("  functions/          # User-defined functions\n")
        f.write("  tables/             # Table CREATE statements\n")
        f.write("```\n")
    
    print(f"\nSummary written to: {summary_path}")


def main():
    print("=" * 60)
    print("SQL OBJECT EXTRACTION - WakeCapDW")
    print("=" * 60)
    
    # Load credentials
    creds = load_credentials()
    mssql_config = creds["mssql"]
    
    print(f"Connecting to: {mssql_config['server']}")
    print(f"Database: {mssql_config['database']}")
    
    # Create database manager
    db_manager = DatabaseManager("mssql", mssql_config)
    
    # Verify connection
    if not db_manager.check_connection():
        print("[FAIL] Could not connect to database")
        return 1
    
    print("[OK] Connected successfully\n")
    
    # Output directory
    output_dir = Path(__file__).parent / "source_sql"
    
    # Extract all objects
    counts = {
        'procedures': extract_stored_procedures(db_manager, output_dir),
        'views': extract_views(db_manager, output_dir),
        'functions': extract_functions(db_manager, output_dir),
        'tables': extract_table_schemas(db_manager, output_dir),
    }
    
    # Generate summary
    generate_summary(output_dir, counts)
    
    print("\n" + "=" * 60)
    print("EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"Total objects extracted: {sum(counts.values())}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
