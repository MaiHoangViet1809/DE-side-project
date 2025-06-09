CASE WHEN {{ column_name }} IS NULL THEN NULL
     ELSE CAST(
     CONCAT(RIGHT(REPLACE(REPLACE(REPLACE({{ column_name }}, '/', '.'), '\', '.'), '-', '.'), 4)
          , '-'
          , SUBSTRING(REPLACE(REPLACE(REPLACE({{ column_name }}, '/', '.'), '\', '.'), '-', '.'), 2, CHARINDEX('.', REPLACE(REPLACE(REPLACE({{ column_name }}, '/', '.'), '\', '.'), '-', '.')) - 2)
          , '-01')
     AS DATETIME)
 END


