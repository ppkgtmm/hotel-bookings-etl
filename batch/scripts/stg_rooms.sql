DELETE
FROM {{ params.rooms }}
WHERE is_deleted = true
