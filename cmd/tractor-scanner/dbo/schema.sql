CREATE TABLE IF NOT EXISTS tractor_events (
  ID           BIGSERIAL                NOT NULL PRIMARY KEY,
  FLOW         TEXT                     NOT NULL,
  EVENT_ID     TEXT,
  EVENT        TEXT                     NOT NULL,
  EVENT_STAMP  TIMESTAMP WITH TIME ZONE,
  SOURCE       TEXT,
  PAYLOAD      BYTEA                    NOT NULL,
  CREATED_AT   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
  FROM_SERVICE TEXT, -- original provider of event
  LAST_SERVICE TEXT, -- last service that processed event (maybe different after retry)
  LAST_ERROR   TEXT, -- last error
  LAST_RETRY   INT, -- previous known retry number
  RETRY        INT   -- current retry number
);

CREATE INDEX IF NOT EXISTS tractor_events_event_id
  ON tractor_events (EVENT_ID);
CREATE INDEX IF NOT EXISTS tractor_events_event_stamp
  ON tractor_events (EVENT_STAMP DESC);
CREATE INDEX IF NOT EXISTS tractor_events_created_at
  ON tractor_events (CREATED_AT DESC);