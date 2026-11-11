-- PikaTimer v2.0: indexes for the high-volume tables.
-- These tables were created without primary keys or indexes, so every
-- lookup by id or by owner was a full table scan.
-- Plain (non-unique) indexes are used on purpose so that this migration
-- can never fail on an existing event file.

-- participant collection tables
CREATE INDEX IF NOT EXISTS idx_part2wave_participant ON part2wave (participant_id);
CREATE INDEX IF NOT EXISTS idx_participant_attributes_participant ON participant_attributes (participant_id);
CREATE INDEX IF NOT EXISTS idx_participant_regeventmap_part ON participant_regeventmap (partID);

-- results
CREATE INDEX IF NOT EXISTS idx_results_id ON results (result_id);
CREATE INDEX IF NOT EXISTS idx_split_results_result ON split_results (result_id);

-- timing data
CREATE INDEX IF NOT EXISTS idx_raw_timing_data_id ON raw_timing_data (id);
CREATE INDEX IF NOT EXISTS idx_raw_timing_data_input ON raw_timing_data (timing_loc_input_id);
CREATE INDEX IF NOT EXISTS idx_cooked_timing_data_id ON cooked_timing_data (id);
CREATE INDEX IF NOT EXISTS idx_cooked_timing_data_input ON cooked_timing_data (timing_loc_input_id);
CREATE INDEX IF NOT EXISTS idx_overrides_id ON overrides (override_id);
CREATE INDEX IF NOT EXISTS idx_bib2chipmap_map ON bib2chipmap (bib2chip_id);
