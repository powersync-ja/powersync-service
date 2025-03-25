-- Create a user with necessary privileges
CREATE USER 'repl_user'@'%' IDENTIFIED BY 'good_password';

-- Grant replication client privilege
GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'repl_user'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'myuser'@'%';

-- Grant access to the specific database
GRANT ALL PRIVILEGES ON mydatabase.* TO 'repl_user'@'%';

-- Apply changes
FLUSH PRIVILEGES;