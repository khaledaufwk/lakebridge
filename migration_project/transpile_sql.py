#!/usr/bin/env python3
"""
Transpile SQL files from T-SQL to Databricks SQL using SQLGlot engine.
"""
import sys
import asyncio
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from databricks.labs.lakebridge.transpiler.sqlglot.sqlglot_engine import SqlglotEngine
from databricks.labs.lakebridge.config import TranspileResult


async def transpile_file(engine: SqlglotEngine, input_path: Path, output_path: Path) -> dict:
    """Transpile a single SQL file."""
    try:
        with open(input_path, 'r', encoding='utf-8', errors='ignore') as f:
            source_code = f.read()
    except Exception as e:
        return {
            'input': str(input_path.name),
            'status': 'error',
            'error': f'Failed to read file: {e}',
            'errors': 1,
            'statements': 0,
        }
    
    # Skip very small files (likely empty or just comments)
    if len(source_code.strip()) < 10:
        return {
            'input': str(input_path.name),
            'status': 'skipped',
            'reason': 'Empty or minimal content',
            'errors': 0,
            'statements': 0,
        }
    
    try:
        result: TranspileResult = await engine.transpile(
            source_dialect='tsql',
            target_dialect='databricks',
            source_code=source_code,
            file_path=input_path
        )
        
        # Write output
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(f"-- Transpiled from: {input_path.name}\n")
            f.write(f"-- Source dialect: T-SQL (SQL Server)\n")
            f.write(f"-- Target dialect: Databricks SQL\n")
            f.write(f"-- Transpiled at: {datetime.now().isoformat()}\n")
            f.write(f"-- Errors: {len(result.error_list)}\n\n")
            f.write(result.transpiled_code)
        
        status = 'success' if not result.error_list else 'partial'
        
        return {
            'input': str(input_path.name),
            'output': str(output_path.name),
            'status': status,
            'errors': len(result.error_list),
            'statements': result.transpiled_statement_count,
            'error_details': [str(e) for e in result.error_list[:5]] if result.error_list else [],
        }
        
    except Exception as e:
        return {
            'input': str(input_path.name),
            'status': 'error',
            'error': str(e)[:200],
            'errors': 1,
            'statements': 0,
        }


async def transpile_directory(engine: SqlglotEngine, input_dir: Path, output_dir: Path, category: str) -> list[dict]:
    """Transpile all SQL files in a directory."""
    results = []
    
    input_path = input_dir / category
    output_path = output_dir / category
    
    if not input_path.exists():
        return results
    
    sql_files = list(input_path.glob('*.sql'))
    total = len(sql_files)
    
    print(f"\n  Transpiling {category}: {total} files")
    
    for i, sql_file in enumerate(sql_files, 1):
        output_file = output_path / sql_file.name
        result = await transpile_file(engine, sql_file, output_file)
        results.append(result)
        
        status_char = '.' if result['status'] == 'success' else 'p' if result['status'] == 'partial' else 'E'
        if i % 20 == 0 or i == total:
            print(f"    [{i}/{total}] {status_char}", end='', flush=True)
    
    print()
    return results


def generate_transpilation_report(all_results: dict, output_dir: Path):
    """Generate a detailed transpilation report."""
    
    # Calculate statistics
    total_files = 0
    success_count = 0
    partial_count = 0
    error_count = 0
    skipped_count = 0
    total_errors = 0
    total_statements = 0
    
    error_types = defaultdict(int)
    
    for category, results in all_results.items():
        for r in results:
            total_files += 1
            if r['status'] == 'success':
                success_count += 1
            elif r['status'] == 'partial':
                partial_count += 1
            elif r['status'] == 'error':
                error_count += 1
            elif r['status'] == 'skipped':
                skipped_count += 1
            
            total_errors += r.get('errors', 0)
            total_statements += r.get('statements', 0)
            
            for err in r.get('error_details', []):
                if 'UNSUPPORTED_SQL' in str(err):
                    error_types['Unsupported SQL'] += 1
                elif 'PARSE_ERROR' in str(err):
                    error_types['Parse Error'] += 1
                elif 'TOKEN_ERROR' in str(err):
                    error_types['Token Error'] += 1
                else:
                    error_types['Other'] += 1
    
    # Generate markdown report
    report = []
    report.append("# Transpilation Results Report\n")
    report.append(f"**Generated:** {datetime.now().isoformat()}\n")
    report.append(f"**Source Dialect:** T-SQL (SQL Server)\n")
    report.append(f"**Target Dialect:** Databricks SQL\n\n")
    
    report.append("## Summary\n")
    report.append(f"| Metric | Count |\n")
    report.append(f"|--------|-------|\n")
    report.append(f"| Total Files | {total_files} |\n")
    report.append(f"| Successful | {success_count} ({100*success_count//max(total_files,1)}%) |\n")
    report.append(f"| Partial | {partial_count} ({100*partial_count//max(total_files,1)}%) |\n")
    report.append(f"| Errors | {error_count} ({100*error_count//max(total_files,1)}%) |\n")
    report.append(f"| Skipped | {skipped_count} |\n")
    report.append(f"| Total Statements | {total_statements} |\n")
    report.append(f"| Total Parse Errors | {total_errors} |\n\n")
    
    report.append("## Results by Category\n")
    for category, results in all_results.items():
        cat_success = sum(1 for r in results if r['status'] == 'success')
        cat_partial = sum(1 for r in results if r['status'] == 'partial')
        cat_error = sum(1 for r in results if r['status'] == 'error')
        cat_total = len(results)
        
        report.append(f"### {category.replace('_', ' ').title()}\n")
        report.append(f"- Total: {cat_total}\n")
        report.append(f"- Successful: {cat_success}\n")
        report.append(f"- Partial: {cat_partial}\n")
        report.append(f"- Errors: {cat_error}\n\n")
    
    if error_types:
        report.append("## Error Types\n")
        report.append("| Error Type | Count |\n")
        report.append("|------------|-------|\n")
        for err_type, count in sorted(error_types.items(), key=lambda x: -x[1]):
            report.append(f"| {err_type} | {count} |\n")
        report.append("\n")
    
    # List files with errors
    report.append("## Files Requiring Manual Review\n")
    report.append("These files had errors during transpilation:\n\n")
    
    error_files = []
    for category, results in all_results.items():
        for r in results:
            if r['status'] == 'error' or (r['status'] == 'partial' and r.get('errors', 0) > 2):
                error_files.append((category, r))
    
    if error_files:
        report.append("| Category | File | Errors |\n")
        report.append("|----------|------|--------|\n")
        for category, r in error_files[:30]:
            report.append(f"| {category} | {r['input']} | {r.get('errors', 1)} |\n")
        
        if len(error_files) > 30:
            report.append(f"| ... | *{len(error_files) - 30} more files* | |\n")
    else:
        report.append("No significant errors found!\n")
    
    report.append("\n## Next Steps\n")
    report.append("1. Review transpiled files in `transpiled/` directory\n")
    report.append("2. Manually fix files listed as requiring review\n")
    report.append("3. Test transpiled SQL in Databricks SQL Editor\n")
    report.append("4. Generate DLT pipeline with `dlt-generate`\n")
    
    # Write report
    report_path = output_dir / "TRANSPILATION_REPORT.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(''.join(report))
    
    # Write JSON results
    json_path = output_dir / "transpilation_results.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump({
            'summary': {
                'total_files': total_files,
                'success': success_count,
                'partial': partial_count,
                'error': error_count,
                'skipped': skipped_count,
                'total_statements': total_statements,
                'total_errors': total_errors,
            },
            'error_types': dict(error_types),
            'results': all_results,
        }, f, indent=2)
    
    return report_path, json_path


async def main():
    print("=" * 60)
    print("SQL TRANSPILATION - T-SQL to Databricks")
    print("=" * 60)
    
    base_dir = Path(__file__).parent
    input_dir = base_dir / "source_sql"
    output_dir = base_dir / "transpiled"
    
    # Ensure output directory exists
    output_dir.mkdir(exist_ok=True)
    
    # Initialize SQLGlot engine
    engine = SqlglotEngine()
    print(f"Using transpiler: {engine.transpiler_name}")
    print(f"Supported dialects: {', '.join(engine.supported_dialects)}")
    
    # Transpile each category
    categories = ['stored_procedures', 'views', 'functions', 'tables']
    all_results = {}
    
    for category in categories:
        results = await transpile_directory(engine, input_dir, output_dir, category)
        if results:
            all_results[category] = results
    
    # Generate report
    print("\nGenerating transpilation report...")
    report_path, json_path = generate_transpilation_report(all_results, output_dir)
    
    # Print summary
    total = sum(len(r) for r in all_results.values())
    success = sum(sum(1 for r in results if r['status'] == 'success') for results in all_results.values())
    partial = sum(sum(1 for r in results if r['status'] == 'partial') for results in all_results.values())
    
    print("\n" + "=" * 60)
    print("TRANSPILATION COMPLETE")
    print("=" * 60)
    print(f"Total files processed: {total}")
    print(f"  - Successful: {success}")
    print(f"  - Partial: {partial}")
    print(f"  - Errors: {total - success - partial}")
    print(f"\nReports generated:")
    print(f"  - {report_path}")
    print(f"  - {json_path}")
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
