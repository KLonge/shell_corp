# Redshift to Snowflake Migration Rules

**Instruction for LLM:** You have been provided with relevant dbt SQL files as context, which may contain SQL that is either not converted or partially converted from Redshift. Please review these files and convert any Redshift-specific syntax to Snowflake syntax where necessary. The notes in this file are only supplemental. You should use them in addition to your knowledge of Snowflake and dbt (not as a replacement).

**Important:** Preserve all existing comments in the SQL code unless they are specifically about Redshift functionality. Comments often contain important business logic explanations and should be maintained during migration.

## Additional Notes
These are supplementary notes to keep in mind when migrating from Redshift to Snowflake, in addition to standard migration practices. These notes are not comprehensive - they only highlight specific differences to be aware of.

## 1. Explicit Column Aliasing with Type Casting
When type casting columns in Snowflake, you **must** include explicit column aliases using `as`, even when the alias matches the original column name. This is not optional - the migration will fail without explicit aliases.

For example, ensure that `event::varchar(128)` is written as `event::varchar(128) as event`.

## 2. Window Functions Require ORDER BY
When using window functions like `ROW_NUMBER` in Snowflake, you **must** include an `ORDER BY` clause within the window specification. Failing to do so will result in a SQL compilation error.

For example, if no specific ordering is needed, you can use `ORDER BY 1`:
`ROW_NUMBER() OVER (PARTITION BY column_name)` should be written as `ROW_NUMBER() OVER (PARTITION BY column_name ORDER BY 1)`.

## 3. JSON Handling Differences
When migrating JSON handling from Redshift to Snowflake, be aware of the differences in functions and syntax. 

- **Redshift**: Uses `json_extract_path_text` to extract JSON values.
- **Snowflake**: Uses `parse_json` in combination with `:` to access JSON fields.

For example:
- Redshift: `json_extract_path_text(json_serialize(serialized_event), 'event', true) as event`
- Snowflake: `parse_json(serialized_event):event as event`

Ensure that JSON paths and functions are correctly converted to maintain the same functionality.

## 4. Regular Expression Differences
When migrating regex patterns from Redshift to Snowflake, be aware of the syntax differences:

- **Redshift**: Uses `~` or `~*` for regex matching and `!~` or `!~*` for regex non-matching
- **Snowflake**: Uses `RLIKE` or `REGEXP` for regex matching

For example:
- Redshift: `column ~ '^[0-9]+$'`
- Snowflake: `column RLIKE '^[0-9]+$'`

Note that Snowflake's `RLIKE` is case-insensitive by default, while Redshift's `~` is case-sensitive (use `~*` for case-insensitive).

### Example Conversion
- **Redshift**: 
  ```sql
  SELECT * FROM table WHERE column ~ '^[A-Z]+$';
  ```
- **Snowflake**: 
  ```sql
  SELECT * FROM table WHERE column RLIKE '^[A-Z]+$';
  ```

In this example, the regex pattern checks for uppercase letters only. The conversion involves replacing `~` with `RLIKE`.

## 5. Unix Timestamp Conversion
When converting Unix timestamps (in milliseconds) to timestamps in Snowflake, use the native `to_timestamp_ntz()` function instead of interval arithmetic. When working with timestamps from JSON, remember to cast VARIANT types to STRING first.

For example:
- **Redshift**: 
  ```sql
  -- From JSON
  json_extract_path_text(serialized_event, 'timestamp') as "timestamp",
  -- Then convert
  CASE
      WHEN "timestamp" ~ '^[0-9]+$' 
          THEN timestamp 'epoch' + "timestamp"::numeric * interval '0.001 second'
      ELSE "timestamp"::timestamp
  END
  ```
- **Snowflake**: 
  ```sql
  -- From JSON
  parse_json(serialized_event):timestamp::string as "timestamp",
  -- Then convert
  CASE
      WHEN "timestamp" rlike '^[0-9]+$'
          THEN to_timestamp_ntz(to_number("timestamp") / 1000)
      ELSE try_to_timestamp("timestamp")
  END
  ```

Note that:
- `try_to_timestamp()` is preferred over direct casting as it handles invalid formats gracefully
- The division by 1000 converts milliseconds to seconds
- `to_timestamp_ntz()` creates a timestamp without time zone
- When parsing from JSON, always cast VARIANT types to STRING before timestamp conversion

## 6. TIMESTAMP Column Handling
When dealing with a column named "timestamp" (lowercase) in the source data, it needs to be:
1. Enclosed in double quotes
2. Uppercase

For example:
- **Original**: 
  ```sql
  timestamp::datetime as event_timestamp
  ```
- **Correct**: 
  ```sql
  "TIMESTAMP"::datetime as event_timestamp
  ```

This is necessary because:
- `timestamp` is a reserved word in Snowflake
- The column name should be consistent with Snowflake's case-sensitive handling
- Double quotes preserve the exact case of identifiers in Snowflake

## 7. Array Expansion (JSON Arrays)
When expanding JSON arrays, the syntax differs significantly between Redshift and Snowflake:

- **Redshift**: Uses `left join` with `on TRUE` to expand arrays
- **Snowflake**: Uses `lateral flatten()` function, which automatically creates a `value` column containing each array element

For example:
- **Redshift**: 
  ```sql
  select
      p.messageid,
      p.eventtimestamp,
      v::varchar as vals
  from table_name p
  left join p.praisevalues v on true
  ```
- **Snowflake**: 
  ```sql
  select
      p.messageid,
      p.eventtimestamp,
      f.value::varchar as vals
  from table_name p,
  lateral flatten(input => p.praisevalues) f
  ```

Note that:
- Snowflake's `FLATTEN` function automatically creates a `value` column containing each array element
- You can alias the flattened result (e.g., `f` in the example) for clarity
- Other useful columns provided by `FLATTEN` include `index` (0-based array position), `key` (for objects), and `seq` (sequence number)

## 8. Converting SUPER to VARIANT

When migrating from Redshift's `SUPER` type to Snowflake's `VARIANT` type, note that the syntax for accessing nested fields changes from using dots (`.`) to colons (`:`).

### Example Conversion

- **Redshift**:
  ```sql
  with feeling as (
      select
          sys."id"::text as feelingcontentfulid,
          sys."createdAt"::datetime as createdat,
          sys."updatedAt"::datetime as updatedat,
          fields."legacyId"."en-GB"::varchar as feelinglegacyid,
          fields."title"."en-GB"::text as feelingtext
      from {{ source('contentful', 'contentful_src_check_in_feeling') }}
  )
  ```

- **Snowflake**:
  ```sql
  with feeling as (
      select
          sys:id::text as feelingcontentfulid,
          sys:createdAt::datetime as createdat,
          sys:updatedAt::datetime as updatedat,
          fields:legacyId:"en-GB"::varchar as feelinglegacyid,
          fields:title:"en-GB"::text as feelingtext
      from {{ source('contentful', 'contentful_src_check_in_feeling') }}
  )
  ```

In this example, the conversion involves replacing dots (`.`) with colons (`:`) when accessing nested fields in JSON-like structures.

## Purpose
This file serves as a guide for converting dbt SQL files from Redshift to Snowflake syntax. When processing SQL files, please convert any Redshift-specific syntax to its Snowflake equivalent while maintaining the same business logic and functionality.
