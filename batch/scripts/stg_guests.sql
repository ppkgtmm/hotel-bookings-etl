DELETE
FROM {{ params.guests }}
WHERE is_deleted = true
