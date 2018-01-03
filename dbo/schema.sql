CREATE TABLE IF NOT EXISTS tractor_result (
  ID              BIGSERIAL                NOT NULL PRIMARY KEY,
  EVENT           TEXT                     NOT NULL,
  EVENT_ID        TEXT                     NOT NULL,
  PARENT_EVENT_ID TEXT,
  STARTED_AT      TIMESTAMP WITH TIME ZONE NOT NULL,
  INPUT           BYTEA                    NOT NULL,
  FINISHED_AT     TIMESTAMP WITH TIME ZONE NOT NULL,
  OUTPUT          BYTEA                    NOT NULL,
  OUTPUT_EVENT_ID TEXT                     NOT NULL,
  JSON_HEADERS    TEXT                     NOT NULL,
  ERR             TEXT
);

CREATE INDEX IF NOT EXISTS tractor_result_event_id
  ON tractor_result (EVENT_ID);
CREATE INDEX IF NOT EXISTS tractor_result_started_at
  ON tractor_result (STARTED_AT DESC);