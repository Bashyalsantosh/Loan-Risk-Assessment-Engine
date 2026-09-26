CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE audit.schema_changes (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    session_user_name TEXT DEFAULT SESSION_USER NOT NULL,
    current_user_name TEXT DEFAULT CURRENT_USER NOT NULL,
    client_ip INET DEFAULT inet_client_addr(),
    command_tag TEXT NOT NULL,
    object_type TEXT,
    object_identity TEXT,
    schema_name TEXT
);
