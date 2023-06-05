CREATE USER 'airbyte'@'%' IDENTIFIED BY 'your_password_here';

GRANT SELECT ON *.* TO 'airbyte'@'%';
