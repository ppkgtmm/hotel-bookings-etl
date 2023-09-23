WITH ranked_rooms AS (
    SELECT id, updated_at, ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) rnum
    FROM {{ params.rooms }}
), to_delete AS (
    SELECT id, updated_at
    FROM ranked_rooms
    WHERE rnum > 3
)

DELETE stg
FROM {{ params.rooms }} stg
INNER JOIN to_delete tbd
ON stg.id = tbd.id AND stg.updated_at = tbd.updated_at
