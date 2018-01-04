CREATE TABLE IF NOT EXISTS tractor_result (
  ID              BIGSERIAL                NOT NULL PRIMARY KEY,
  JSON_HEADERS    TEXT                     NOT NULL,
  FLOW            TEXT                     NOT NULL,
  EVENT           TEXT                     NOT NULL,
  EVENT_ID        TEXT                     NOT NULL,
  PARENT_EVENT_ID TEXT,
  STARTED_AT      TIMESTAMP WITH TIME ZONE NOT NULL,
  INPUT           BYTEA                    NOT NULL,
  FINISHED_AT     TIMESTAMP WITH TIME ZONE NOT NULL,
  OUTPUT          BYTEA                    NOT NULL,
  NODE            TEXT                     NOT NULL,
  ERR             TEXT
);

CREATE INDEX IF NOT EXISTS tractor_result_event_id
  ON tractor_result (EVENT_ID);
CREATE INDEX IF NOT EXISTS tractor_result_flow
  ON tractor_result (FLOW);
CREATE INDEX IF NOT EXISTS tractor_result_node
  ON tractor_result (NODE);
CREATE INDEX IF NOT EXISTS tractor_result_started_at
  ON tractor_result (STARTED_AT DESC);