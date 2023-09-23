DELETE b
FROM {{ params.bookings }} b
WHERE b.is_deleted = true OR NOT EXISTS (
    SELECT br.id 
    FROM {{ params.booking_rooms }} br
    WHERE br.booking = b.id AND br.processed = false AND br.is_deleted = false
);
