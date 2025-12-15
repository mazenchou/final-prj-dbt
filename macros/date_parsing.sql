-- macros/date_parsing.sql
{% macro parse_date_any_format(date_column) %}
  {# Universal date parser that handles multiple formats #}
  CASE 
    WHEN {{ date_column }} IS NULL OR {{ date_column }} = '' THEN NULL
    -- Try YYYY/MM/DD first (your likely format)
    WHEN SAFE.PARSE_DATE('%Y/%m/%d', CAST({{ date_column }} AS STRING)) IS NOT NULL 
      THEN SAFE.PARSE_DATE('%Y/%m/%d', CAST({{ date_column }} AS STRING))
    -- Try YYYY-MM-DD second  
    WHEN SAFE.PARSE_DATE('%Y-%m-%d', CAST({{ date_column }} AS STRING)) IS NOT NULL 
      THEN SAFE.PARSE_DATE('%Y-%m-%d', CAST({{ date_column }} AS STRING))
    -- Try YYYYMMDD (integer format)
    WHEN SAFE.PARSE_DATE('%Y%m%d', CAST({{ date_column }} AS STRING)) IS NOT NULL 
      THEN SAFE.PARSE_DATE('%Y%m%d', CAST({{ date_column }} AS STRING))
    ELSE NULL
  END
{% endmacro %}

{% macro parse_datetime_any_format(datetime_column) %}
  {# Universal datetime parser #}
  CASE 
    WHEN {{ datetime_column }} IS NULL OR {{ datetime_column }} = '' THEN NULL
    WHEN SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', CAST({{ datetime_column }} AS STRING)) IS NOT NULL 
      THEN SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', CAST({{ datetime_column }} AS STRING))
    WHEN SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', CAST({{ datetime_column }} AS STRING)) IS NOT NULL 
      THEN SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', CAST({{ datetime_column }} AS STRING))
    ELSE NULL
  END
{% endmacro %}