# dbt Unique Key Implementation Guide

**Instruction for LLM:** You are tasked with adding unique key configurations to dbt models. Review the provided SQL files and add appropriate unique keys based on the business logic and data structure.

## Key Guidelines

1. **Config Structure**
   - Every model should have a config block
   - Unique keys should be specified in the config using the `unique_key` parameter
   - Use array format for multi-column keys: `unique_key=['col1', 'col2']`

2. **Column Selection Priority**
   - Primary business identifiers (e.g., order_id, user_id)
   - Natural composite keys (e.g., [date, product_id, location_id])
   - System-generated unique identifiers
   - Maximum of 5 columns per unique key

3. **Common Patterns**
   ```sql
   {{ config(
       materialized='table',
       unique_key='id'
   ) }}

   {{ config(
       materialized='incremental',
       unique_key=['date', 'customer_id']
   ) }}
   ```

## Examples

### Single Column Key
```sql
-- Original
{{ config(
    materialized='table'
) }}

-- Modified
{{ config(
    materialized='table',
    unique_key='order_id'
) }}
```

### Composite Key
```sql
-- Original
{{ config(
    materialized='incremental'
) }}

-- Modified
{{ config(
    materialized='incremental',
    unique_key=['date', 'product_id', 'location_id']
) }}
```

Remember to consider the business context and data relationships when selecting unique key columns. When in doubt, include additional relevant columns while staying within the 5-column limit.