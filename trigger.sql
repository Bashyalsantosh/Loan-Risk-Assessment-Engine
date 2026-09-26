-- Trigger function invoked upon DDL completion
CREATE OR REPLACE FUNCTION audit.log_ddl_event()
RETURNS event_trigger AS $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        INSERT INTO audit.schema_changes (
            command_tag,
            object_type,
            object_identity,
            schema_name
        ) VALUES (
            obj.command_tag,
            obj.object_type,
            obj.object_identity,
            obj.schema_name
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Create the event trigger targeting ddl_command_end events
CREATE EVENT TRIGGER trg_audit_ddl_commands
    ON ddl_command_end
    EXECUTE FUNCTION audit.log_ddl_event();
