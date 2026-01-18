#!/usr/bin/env python3
"""
Analyze extracted SQL files for migration assessment.
"""
import sys
import json
from pathlib import Path
from collections import defaultdict

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import sqlglot
from sqlglot import exp


def analyze_sql_file(filepath: Path) -> dict:
    """Analyze a single SQL file and return metrics."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        return {'error': str(e), 'path': str(filepath)}
    
    # Skip header comments to get actual SQL
    lines = content.split('\n')
    sql_start = 0
    for i, line in enumerate(lines):
        if not line.strip().startswith('--') and line.strip():
            sql_start = i
            break
    
    sql_content = '\n'.join(lines[sql_start:])
    
    metrics = {
        'path': str(filepath.name),
        'size_bytes': len(content),
        'line_count': len(lines),
        'statements': [],
        'tables_referenced': set(),
        'columns_referenced': set(),
        'functions_used': set(),
        'has_cursor': False,
        'has_temp_table': False,
        'has_dynamic_sql': False,
        'has_transaction': False,
        'complexity_indicators': [],
        'parse_status': 'success',
        'parse_errors': []
    }
    
    # Try to parse with SQLGlot
    try:
        statements = list(sqlglot.parse(sql_content, dialect='tsql'))
        
        for stmt in statements:
            if stmt is None:
                continue
                
            stmt_type = type(stmt).__name__
            metrics['statements'].append(stmt_type)
            
            # Find all table references
            for table in stmt.find_all(exp.Table):
                if table.name:
                    metrics['tables_referenced'].add(table.name)
            
            # Find all column references
            for col in stmt.find_all(exp.Column):
                if col.name:
                    metrics['columns_referenced'].add(col.name)
            
            # Find all function calls
            for func in stmt.find_all(exp.Func):
                metrics['functions_used'].add(type(func).__name__)
    
    except Exception as e:
        metrics['parse_status'] = 'partial'
        metrics['parse_errors'].append(str(e)[:200])
    
    # Text-based analysis for patterns that might not parse
    content_lower = content.lower()
    
    if 'cursor' in content_lower:
        metrics['has_cursor'] = True
        metrics['complexity_indicators'].append('CURSOR')
    
    if '#' in content or 'tempdb' in content_lower:
        metrics['has_temp_table'] = True
        metrics['complexity_indicators'].append('TEMP_TABLE')
    
    if 'exec(' in content_lower or 'execute(' in content_lower or 'sp_executesql' in content_lower:
        metrics['has_dynamic_sql'] = True
        metrics['complexity_indicators'].append('DYNAMIC_SQL')
    
    if 'begin transaction' in content_lower or 'begin tran' in content_lower:
        metrics['has_transaction'] = True
        metrics['complexity_indicators'].append('TRANSACTION')
    
    if 'merge ' in content_lower:
        metrics['complexity_indicators'].append('MERGE')
    
    if 'pivot' in content_lower or 'unpivot' in content_lower:
        metrics['complexity_indicators'].append('PIVOT')
    
    if 'try_convert' in content_lower or 'try_cast' in content_lower:
        metrics['complexity_indicators'].append('TRY_CONVERT')
    
    if 'geography' in content_lower or 'geometry' in content_lower:
        metrics['complexity_indicators'].append('SPATIAL')
    
    if 'row_number()' in content_lower or 'rank()' in content_lower or 'dense_rank()' in content_lower:
        metrics['complexity_indicators'].append('WINDOW_FUNCTION')
    
    if 'cte' in content_lower or 'with ' in content_lower[:500]:
        metrics['complexity_indicators'].append('CTE')
    
    # Convert sets to lists for JSON serialization
    metrics['tables_referenced'] = sorted(list(metrics['tables_referenced']))
    metrics['columns_referenced'] = sorted(list(metrics['columns_referenced']))[:50]  # Limit
    metrics['functions_used'] = sorted(list(metrics['functions_used']))
    
    return metrics


def analyze_directory(source_dir: Path) -> dict:
    """Analyze all SQL files in a directory."""
    results = {
        'procedures': [],
        'views': [],
        'functions': [],
        'tables': [],
    }
    
    categories = {
        'stored_procedures': 'procedures',
        'views': 'views',
        'functions': 'functions',
        'tables': 'tables',
    }
    
    for subdir, category in categories.items():
        dir_path = source_dir / subdir
        if dir_path.exists():
            for sql_file in dir_path.glob('*.sql'):
                analysis = analyze_sql_file(sql_file)
                results[category].append(analysis)
    
    return results


def generate_assessment_report(results: dict, output_path: Path):
    """Generate a detailed assessment report."""
    
    total_objects = sum(len(v) for v in results.values())
    
    # Calculate summary statistics
    all_objects = []
    for category in results.values():
        all_objects.extend(category)
    
    complexity_counts = defaultdict(int)
    for obj in all_objects:
        for indicator in obj.get('complexity_indicators', []):
            complexity_counts[indicator] += 1
    
    parse_success = sum(1 for obj in all_objects if obj.get('parse_status') == 'success')
    parse_partial = sum(1 for obj in all_objects if obj.get('parse_status') == 'partial')
    
    all_tables = set()
    for obj in all_objects:
        all_tables.update(obj.get('tables_referenced', []))
    
    report = []
    report.append("# WakeCapDW Migration Assessment Report\n")
    report.append(f"**Generated for:** WakeCapDW_20251215\n")
    report.append(f"**Source:** Azure SQL Server\n\n")
    
    report.append("## Executive Summary\n")
    report.append(f"Total SQL objects analyzed: **{total_objects}**\n\n")
    report.append("| Category | Count |\n")
    report.append("|----------|-------|\n")
    report.append(f"| Stored Procedures | {len(results['procedures'])} |\n")
    report.append(f"| Views | {len(results['views'])} |\n")
    report.append(f"| Functions | {len(results['functions'])} |\n")
    report.append(f"| Tables | {len(results['tables'])} |\n\n")
    
    report.append("## Parse Analysis\n")
    report.append(f"- Successfully parsed: {parse_success} ({100*parse_success//max(total_objects,1)}%)\n")
    report.append(f"- Partially parsed: {parse_partial} ({100*parse_partial//max(total_objects,1)}%)\n\n")
    
    report.append("## Complexity Indicators\n")
    report.append("These patterns require special attention during migration:\n\n")
    report.append("| Pattern | Occurrences | Migration Impact |\n")
    report.append("|---------|-------------|------------------|\n")
    
    impact_map = {
        'CURSOR': 'High - Replace with set-based operations',
        'TEMP_TABLE': 'Medium - Use Delta temp tables',
        'DYNAMIC_SQL': 'High - Review and rewrite',
        'TRANSACTION': 'Medium - Use Delta transactions',
        'MERGE': 'Low - Supported via MERGE INTO',
        'PIVOT': 'Low - Use pivot/unpivot functions',
        'TRY_CONVERT': 'Low - Use try_cast()',
        'SPATIAL': 'High - Use H3 or custom UDFs',
        'WINDOW_FUNCTION': 'Low - Fully supported',
        'CTE': 'Low - Fully supported',
    }
    
    for indicator, count in sorted(complexity_counts.items(), key=lambda x: -x[1]):
        impact = impact_map.get(indicator, 'Medium - Review required')
        report.append(f"| {indicator} | {count} | {impact} |\n")
    
    report.append("\n## Tables Referenced\n")
    report.append(f"Total unique tables referenced: **{len(all_tables)}**\n\n")
    report.append("Key tables (most referenced):\n")
    
    table_counts = defaultdict(int)
    for obj in all_objects:
        for table in obj.get('tables_referenced', []):
            table_counts[table] += 1
    
    top_tables = sorted(table_counts.items(), key=lambda x: -x[1])[:20]
    for table, count in top_tables:
        report.append(f"- `{table}` (referenced {count} times)\n")
    
    report.append("\n## Stored Procedures Analysis\n")
    if results['procedures']:
        complex_procs = [p for p in results['procedures'] if len(p.get('complexity_indicators', [])) >= 2]
        report.append(f"- Total: {len(results['procedures'])}\n")
        report.append(f"- Complex (2+ indicators): {len(complex_procs)}\n\n")
        
        if complex_procs:
            report.append("### Complex Stored Procedures (Requires Manual Review)\n")
            report.append("| Procedure | Indicators | Lines |\n")
            report.append("|-----------|------------|-------|\n")
            for proc in sorted(complex_procs, key=lambda x: -len(x.get('complexity_indicators', [])))[:15]:
                indicators = ', '.join(proc.get('complexity_indicators', []))
                report.append(f"| {proc['path']} | {indicators} | {proc.get('line_count', 0)} |\n")
    
    report.append("\n## Views Analysis\n")
    if results['views']:
        report.append(f"- Total: {len(results['views'])}\n")
        complex_views = [v for v in results['views'] if len(v.get('complexity_indicators', [])) >= 1]
        report.append(f"- With complexity indicators: {len(complex_views)}\n")
    
    report.append("\n## Functions Analysis\n")
    if results['functions']:
        report.append(f"- Total: {len(results['functions'])}\n")
        scalar_funcs = [f for f in results['functions'] if 'SCALAR' in f['path'].upper() or 'fn' in f['path'].lower()]
        report.append(f"- Scalar functions: ~{len(scalar_funcs)}\n")
        report.append("\n**Note:** User-defined functions will need to be converted to Databricks SQL functions or Python UDFs.\n")
    
    report.append("\n## Migration Recommendations\n")
    report.append("1. **Cursors**: " + str(complexity_counts.get('CURSOR', 0)) + " objects use cursors. Convert to set-based operations.\n")
    report.append("2. **Dynamic SQL**: " + str(complexity_counts.get('DYNAMIC_SQL', 0)) + " objects use dynamic SQL. Review security implications.\n")
    report.append("3. **Spatial Functions**: " + str(complexity_counts.get('SPATIAL', 0)) + " objects use spatial data. Consider H3 library.\n")
    report.append("4. **Transactions**: Evaluate which procedures need Delta Lake transaction guarantees.\n")
    report.append("5. **Temp Tables**: Replace with Delta temporary views or CTEs where possible.\n")
    
    report.append("\n## Next Steps\n")
    report.append("1. Run transpilation: `databricks labs lakebridge transpile --source-dialect tsql`\n")
    report.append("2. Review transpilation errors and warnings\n")
    report.append("3. Manually address high-complexity objects\n")
    report.append("4. Generate DLT pipeline\n")
    report.append("5. Deploy and validate\n")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(''.join(report))
    
    # Also save JSON for programmatic use
    json_path = output_path.with_suffix('.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump({
            'summary': {
                'total_objects': total_objects,
                'procedures': len(results['procedures']),
                'views': len(results['views']),
                'functions': len(results['functions']),
                'tables': len(results['tables']),
                'parse_success_rate': parse_success / max(total_objects, 1),
            },
            'complexity_counts': dict(complexity_counts),
            'tables_referenced': list(all_tables),
            'detailed_results': results,
        }, f, indent=2)
    
    return output_path, json_path


def main():
    print("=" * 60)
    print("SQL MIGRATION ASSESSMENT")
    print("=" * 60)
    
    source_dir = Path(__file__).parent / "source_sql"
    output_dir = Path(__file__).parent / "assessment"
    output_dir.mkdir(exist_ok=True)
    
    print(f"Analyzing SQL files in: {source_dir}")
    
    # Analyze all files
    results = analyze_directory(source_dir)
    
    total = sum(len(v) for v in results.values())
    print(f"\nAnalyzed {total} SQL objects:")
    print(f"  - Stored Procedures: {len(results['procedures'])}")
    print(f"  - Views: {len(results['views'])}")
    print(f"  - Functions: {len(results['functions'])}")
    print(f"  - Tables: {len(results['tables'])}")
    
    # Generate report
    report_path = output_dir / "migration_assessment.md"
    md_path, json_path = generate_assessment_report(results, report_path)
    
    print(f"\n[OK] Assessment report generated:")
    print(f"  - Markdown: {md_path}")
    print(f"  - JSON: {json_path}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
