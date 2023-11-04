SELECT
  date,
  guest,
  guest_gender,
  age_range,
  guest_country,
  guest_state,
  fips,
  addons.addon,
  addons.quantity,
  addons.unit_price
FROM full_picture,
JSON_TABLE(addons, '$[*]' COLUMNS (
    addon VARCHAR(255) PATH '$.addon',
    unit_price INTEGER PATH '$.price',
    quantity INTEGER PATH '$.quantity'
  )
) addons;
