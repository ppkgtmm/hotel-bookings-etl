SELECT
  date,
  guest,
  guest_gender,
  age_range,
  guest_country,
  room_type,
  IF(ROW_NUMBER() OVER(PARTITION BY date, guest) = 1, price, 0) price,
  addons.addon addon_name,
  addons.quantity addon_quantity,
  addons.unit_price addon_unit_price
FROM full_picture
LEFT JOIN JSON_TABLE(addons, '$[*]' COLUMNS (
    addon VARCHAR(255) PATH '$.addon',
    unit_price INTEGER PATH '$.price',
    quantity INTEGER PATH '$.quantity'
  )
) addons
ON TRUE;
