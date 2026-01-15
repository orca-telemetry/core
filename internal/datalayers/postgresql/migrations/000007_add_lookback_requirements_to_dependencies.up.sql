ALTER TABLE algorithm_dependency ADD COLUMN lookback_count BIGINT CHECK (lookback_count >= 0);
ALTER TABLE algorithm_dependency ADD COLUMN lookback_timedelta BIGINT CHECK (lookback_timedelta >= 0);
