WITH ranked_guests AS (
    SELECT id, updated_at, ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rnum
    FROM {{ params.guests }}
), to_delete AS (
    SELECT id, updated_at
    FROM ranked_guests
    WHERE rnum > 3
)

DELETE stg
FROM {{ params.guests }} stg
INNER JOIN to_delete tbd
ON stg.id = tbd.id AND stg.updated_at = tbd.updated_at
