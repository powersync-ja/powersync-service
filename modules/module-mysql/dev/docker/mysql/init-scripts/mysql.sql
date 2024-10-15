-- Create a user with necessary privileges
CREATE USER 'repl_user'@'%' IDENTIFIED BY 'good_password';

-- Grant replication client privilege
GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'repl_user'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'myuser'@'%';

-- Grant access to the specific database
GRANT ALL PRIVILEGES ON mydatabase.* TO 'repl_user'@'%';

-- Apply changes
FLUSH PRIVILEGES;

CREATE TABLE lists (
    id CHAR(36) NOT NULL DEFAULT (UUID()), -- String UUID (36 characters)
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    name TEXT NOT NULL,
    owner_id  CHAR(36) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE todos (
    id CHAR(36) NOT NULL DEFAULT (UUID()), -- String UUID (36 characters)
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    description TEXT NOT NULL,
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    created_by  CHAR(36) NULL,
    completed_by  CHAR(36) NULL,
    list_id  CHAR(36) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (list_id) REFERENCES lists (id) ON DELETE CASCADE
);

-- TODO fix case where no data is present
INSERT INTO lists (id, name, owner_id)
VALUES 
    (UUID(), 'Do a demo', UUID());