CREATE TABLE IF NOT EXISTS flow (
  id         BIGSERIAL PRIMARY KEY    NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
  name       TEXT                     NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS service (
  id         BIGSERIAL PRIMARY KEY    NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
  name       TEXT                     NOT NULL UNIQUE
);


CREATE TABLE IF NOT EXISTS event (
  id         BIGSERIAL PRIMARY KEY    NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
  name       TEXT                     NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS history (
  id              BIGSERIAL PRIMARY KEY    NOT NULL,
  created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
  event_id        BIGINT                   NOT NULL REFERENCES event (id),
  event_msgid     TEXT                     NOT NULL,
  event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
  flow_id         BIGINT                   NOT NULL REFERENCES flow (id),
  from_service_id BIGINT                   NOT NULL REFERENCES service (id),
  last_service_id BIGINT                   NOT NULL REFERENCES service (id),
  repeat          INT                      NOT NULL DEFAULT 0,
  json_headers    TEXT                     NOT NULL,
  body            BYTEA                    NOT NULL
);


CREATE OR REPLACE VIEW history_with_details AS
  SELECT
    history.*,
    event.name       AS event_name,
    event.created_at AS event_created_at,
    flow.name        AS flow_name,
    flow.created_at  AS flow_created_at,
    sf.name          AS from_service_name,
    sf.created_at    AS from_service_created_at,
    sl.name          AS last_service_name,
    sl.created_at    AS last_service_created_at
  FROM history
    INNER JOIN event ON history.event_id = event.id
    INNER JOIN flow ON history.flow_id = flow.id
    INNER JOIN service sf ON history.from_service_id = sf.id
    INNER JOIN service sl ON history.last_service_id = sl.id;


CREATE INDEX IF NOT EXISTS history_event_msgid_key
  ON history (event_msgid);