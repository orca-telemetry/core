-- Drop triggers
DROP TRIGGER IF EXISTS trigger_refresh_algorithm_struct_result_fields_on_references 
ON struct_result_field_references;

DROP TRIGGER IF EXISTS trigger_refresh_algorithm_struct_result_fields_on_processor 
ON processor;

DROP TRIGGER IF EXISTS trigger_refresh_window_type_metadata_fields_on_window_type 
ON window_type;

-- Drop the trigger function
DROP FUNCTION IF EXISTS refresh_window_type_metadata_fields();

-- Drop the materialized view
DROP MATERIALIZED VIEW IF EXISTS window_type_metadata_fields;

-- Drop the bridge table
DROP TABLE IF EXISTS metadata_fields_references;

-- Drop the metadata fields table
DROP TABLE IF EXISTS metadata_fields;
