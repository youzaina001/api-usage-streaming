CREATE ROLE installer LOGIN PASSWORD 'usage-installer-local';
CREATE ROLE usage_consumer LOGIN PASSWORD 'usage-consumer-local';
CREATE ROLE reporting_reader LOGIN PASSWORD 'usage-reporting-local';
ALTER DATABASE usage OWNER TO installer;
