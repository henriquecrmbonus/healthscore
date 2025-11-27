# HealthScore - AI Coding Agent Guidelines

## Project Purpose
HealthScore is a MySQL-based health metrics reporting system that generates consolidated business analytics from a customer loyalty/engagement platform. It extracts multi-source campaign performance data and exports it as CSV reports.

## Architecture & Data Flow

### Core Components
- **Database**: Remote MySQL (`giver` database at 45.79.204.253`)
- **Main Script**: `health_score.py` - orchestrates all queries and CSV export
- **Data Sources**: Multiple interconnected tables tracking campaigns, orders, stores, and customer conversions

### Query Pattern Architecture
The script uses a **template-based parameterized query system**:
- Queries stored as constants (e.g., `QUERY_CAMPANHAS_CRIADAS`)
- Parameter placeholders: `{{brand_id}}`, `'{{date_begin}}'`, `'{{date_until}}'`
- Runtime substitution happens in `execute_query()` with `%s` placeholders for SQL injection prevention

### Data Output Flow
1. User provides: `BRAND_ID`, `DATE_BEGIN`, `DATE_UNTIL` via console input
2. Nine queries execute sequentially, each returning headers + data tuples
3. Results aggregated into `all_query_results` list as dicts
4. Single CSV export via `export_queries_to_single_csv()` with section separators

## Project-Specific Patterns

### Database Connection Management
- Configuration centralized in `DB_CONFIG` dict (credentials hardcoded)
- Connection pooling not implemented - new connection per query
- Proper cleanup: explicit `cursor.close()` and `conn.close()` in finally blocks
- Error handling catches both `mysql.connector.Error` and generic exceptions

### Query Execution Pattern
```python
def execute_query(query, params):
    # Converts template placeholders to parameterized SQL
    # Returns tuple: (headers, data) - empty lists on error
    # Always returns predictable structure regardless of outcome
```

### CSV Export Structure
- `export_queries_to_single_csv()` creates multi-table CSV with blank row separators
- Each section: query name header → column headers → data rows
- Uses UTF-8 encoding to handle Portuguese characters in table names

## Critical Queries & Metrics
The script generates 9 key business metrics:

1. **QUANTIDADE_DE_CAMPANHAS_CRIADAS** - Campaign count by type (Email, SMS, Agenda, Cashback)
2. **BASE_IMPACTADA_POR_CANAL** - Customer count reached by delivery channel
3. **NÚMERO_DE_LOJAS** - Store count by status (active/onboarding)
4. **LOJAS_INADIMPLENTES** - Delinquent stores (name + CNPJ)
5. **RFU_CAMPANHAS** - Revenue from campaign conversions
6. **RFU_CASHBACK** - Revenue from cashback transactions
7. **RFU_TOTAL** - Combined all-channel revenue
8. **RFU_TELEMARKETING** - Telemarketing-sourced revenue
9. **RFU_GATILHOS** - Trigger/automated email revenue

**Note**: RFU (Return From Users) queries use complex nested subqueries with CASE logic for store status validation and resource type filtering. Search for `cli_order_convertion` to understand the core join pattern.

## Development Workflows

### Running the Script
```bash
python health_score.py
# Prompts for: BRAND_ID (integer), DATE_BEGIN (YYYY-MM-DD), DATE_UNTIL (YYYY-MM-DD)
# Outputs: relatorio_consolidado_mysql.csv
```

### Adding New Queries
1. Define query constant following existing naming: `QUERY_<METRIC_NAME_UPPER>`
2. Use template syntax: `brand_id = {{brand_id}}`, `'{{date_begin}}'`, `'{{date_until}}'`
3. Add execution block in main section:
   ```python
   headers_n, data_n = execute_query(QUERY_NAME, {'brand_id': BRAND_ID, ...})
   all_query_results.append({'name': 'READABLE_NAME', 'headers': headers_n, 'data': data_n})
   ```
4. Maintain order - output CSV follows execution sequence

### Debugging Database Issues
- Check credentials in `DB_CONFIG` match active database state
- Verify date parameters in YYYY-MM-DD format
- Brand_id must exist in `cli_brand` table
- Monitor MySQL connection limits - new connection per query
- Error messages printed to console but don't halt execution (graceful degradation)

## Key Integration Points

### External Dependencies
- `mysql.connector` - must be installed: `pip install mysql-connector-python`
- Standard library: `csv`, no external ETL frameworks

### Database Schema References
Core tables referenced:
- `cli_campaign` - campaign definitions and metadata
- `cli_campaign_return` - delivery tracking data
- `cli_store` - store master data with status
- `cli_order_convertion` - conversion attribution (critical join hub)
- `cli_transaction` - transaction/cashback records
- `cli_brand` - brand master table

## Security & Maintenance Notes
- Credentials stored in plaintext (security risk) - move to environment variables in production
- No query logging - difficult to debug failed queries after-the-fact
- CSV exports contain no data validation or integrity checks
- Should implement connection pooling for multiple daily executions
