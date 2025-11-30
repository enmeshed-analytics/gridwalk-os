CREATE SCHEMA gridwalk;
CREATE SCHEMA gridwalk_layer_data;

-- Trigger to auto-update `updated_at`
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Layers table
CREATE TABLE gridwalk.layers (
    id UUID PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    upload_type VARCHAR(100),
    total_size BIGINT NOT NULL DEFAULT 0,
    current_offset BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
