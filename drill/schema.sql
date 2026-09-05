-- searchable_content plus an outbox carrying xact_id.
--
-- xact_id rather than seq alone: sequence values are handed out at statement
-- time and are not consumed in commit order, so a row can become visible
-- with a seq below one already observed. Comparing against a snapshot xmin
-- covers everything that was still in flight.

DROP TABLE IF EXISTS reindex_progress;
DROP TABLE IF EXISTS reindex_outbox;
DROP TABLE IF EXISTS searchable_content;

CREATE TABLE searchable_content (
    id         bigint PRIMARY KEY,
    title      text        NOT NULL,
    body       text        NOT NULL,
    status     text        NOT NULL DEFAULT 'PUBLISHED',
    version    integer     NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX searchable_content_status_idx ON searchable_content (status);
CREATE INDEX searchable_content_updated_at_idx ON searchable_content (updated_at);

CREATE TABLE reindex_outbox (
    seq         bigserial PRIMARY KEY,
    content_id  bigint      NOT NULL,
    op          text        NOT NULL CHECK (op IN ('upsert', 'delete')),
    -- assigned at statement time, visible only at commit time
    xact_id     bigint      NOT NULL DEFAULT pg_current_xact_id()::text::bigint,
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX reindex_outbox_xact_idx ON reindex_outbox (xact_id);
CREATE INDEX reindex_outbox_content_idx ON reindex_outbox (content_id);

-- Durable applied position, one row per drained outbox entry. The drain
-- anti-joins this instead of carrying a high-water seq cursor, which a
-- transaction committing out of sequence order can overtake.
-- Keyed by (rebuild_id, seq): a single global seq key would let one rebuild
-- see another's applied marks, and would break overlapping rebuilds or
-- multiple target index generations.
CREATE TABLE reindex_progress (
    rebuild_id text   NOT NULL,
    seq        bigint NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (rebuild_id, seq)
);

CREATE OR REPLACE FUNCTION capture_outbox() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO reindex_outbox (content_id, op) VALUES (OLD.id, 'delete');
    ELSE
        -- 'upsert' even for PUBLISHED -> DRAFT; the replay re-reads the row
        -- and applies the predicate, so it becomes a delete there.
        INSERT INTO reindex_outbox (content_id, op) VALUES (NEW.id, 'upsert');
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER searchable_content_outbox
    AFTER INSERT OR UPDATE OR DELETE ON searchable_content
    FOR EACH ROW EXECUTE FUNCTION capture_outbox();

-- Ground truth for the negative check, since the rows themselves are gone.
DROP TABLE IF EXISTS workload_deletes;
CREATE TABLE workload_deletes (
    id        bigint PRIMARY KEY,
    run_label text NOT NULL,
    at        timestamptz NOT NULL DEFAULT clock_timestamp()
);
