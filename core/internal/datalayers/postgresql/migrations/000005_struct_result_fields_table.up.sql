-- All metadata fields used by window types
CREATE TABLE struct_result_fields (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL
);

-- Bridge table to handle struct result fields
CREATE TABLE struct_result_field_references (
    algorithm_id BIGINT REFERENCES algorithm(id) ON DELETE CASCADE,
    struct_result_field_id BIGINT REFERENCES struct_result_fields(id) ON DELETE CASCADE,
    PRIMARY KEY (algorithm_id, struct_result_field_id)
);

-- Materialised view to track algorithms, result struct fields and processors in one table
CREATE MATERIALIZED VIEW algorithm_struct_result_fields AS
SELECT 
    a.name AS algorithm_name,
    a.version AS algorithm_version,
    p.name as processor_name,
    p.runtime as processor_runtime,
    srf.name as struct_result_field_name
FROM algorithm a
INNER JOIN struct_result_field_references srfr ON srfr.algorithm_id = a.id
INNER JOIN struct_result_fields srf ON srf.id = srfr.struct_result_field_id
INNER JOIN processor p ON a.processor_id = p.id
ORDER BY a.name, a.version, p.name;

-- Function to refresh the materialised view
CREATE OR REPLACE FUNCTION refresh_algorithm_struct_result_fields()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW algorithm_struct_result_fields;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger on algorithm table
CREATE TRIGGER trigger_refresh_algorithm_struct_result_fields_on_algorithm
    AFTER INSERT OR UPDATE OR DELETE ON algorithm
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_algorithm_struct_result_fields();

-- Trigger on processor table
CREATE TRIGGER trigger_refresh_algorithm_struct_result_fields_on_processor
    AFTER INSERT OR UPDATE OR DELETE ON processor
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_algorithm_struct_result_fields();

-- Trigger on the bridge table
CREATE TRIGGER trigger_refresh_algorithm_struct_result_fields_on_references
    AFTER INSERT OR UPDATE OR DELETE ON struct_result_field_references
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_algorithm_struct_result_fields();

-- TODO: inedexes (?)
