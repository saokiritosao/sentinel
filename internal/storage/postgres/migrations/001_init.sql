-- нужен чтобы severity при апсерте могла только расти
CREATE FUNCTION severity_rank(s TEXT) RETURNS INT
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE s
        WHEN 'low' THEN 1
        WHEN 'medium' THEN 2
        WHEN 'high' THEN 3
        WHEN 'critical' THEN 4
        ELSE 0
    END
$$;

CREATE TABLE alerts (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rule        TEXT        NOT NULL,
    severity    TEXT        NOT NULL CHECK (severity_rank(severity) > 0),
    entity      TEXT        NOT NULL,
    dedup_key   TEXT        NOT NULL,
    title       TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'open'
                CHECK (status IN ('open', 'acknowledged', 'resolved')),
    event_count INT         NOT NULL DEFAULT 0,
    details     JSONB       NOT NULL DEFAULT '{}',
    first_seen  TIMESTAMPTZ NOT NULL,
    last_seen   TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- один активный алерт на (rule, entity), resolved не учитываются
CREATE UNIQUE INDEX alerts_active_dedup_uq ON alerts (dedup_key) WHERE status <> 'resolved';

CREATE INDEX alerts_status_id_idx ON alerts (status, id DESC);
CREATE INDEX alerts_entity_idx    ON alerts (entity);

-- PK не даёт посчитать одно событие дважды при повторной доставке
CREATE TABLE alert_events (
    alert_id    BIGINT      NOT NULL REFERENCES alerts (id) ON DELETE CASCADE,
    event_id    UUID        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (alert_id, event_id)
);
