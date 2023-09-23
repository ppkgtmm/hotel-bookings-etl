DELETE 
FROM {{ params.booking_addons }}
WHERE is_deleted = true OR processed = true;
