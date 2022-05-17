CREATE SCHEMA IF NOT EXISTS eg;
SET SEARCH_PATH = eg;

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS hstore;

CREATE FUNCTION raise_exception(what VARCHAR)
  RETURNS VOID AS
$$
BEGIN
  RAISE EXCEPTION '%', what;
END
$$ LANGUAGE plpgsql;

-- version 2 of tuid_generate is just random
CREATE OR REPLACE FUNCTION tuid_generate()
  RETURNS UUID AS
$$
declare
  r bytea;
  ts bigint;
  ret varchar;
begin
  r := func.gen_random_bytes(10);
  ts := extract(epoch from clock_timestamp() at time zone 'utc') * 1000;

  ret := lpad(to_hex(ts), 12, '0') ||
    lpad(encode(r, 'hex'), 20, '0');

  return ret :: uuid;
end;
$$ LANGUAGE plpgsql;

CREATE FUNCTION tuid_zero()
  RETURNS UUID
  IMMUTABLE
  LANGUAGE sql AS
'SELECT
  ''00000000-0000-0000-0000-000000000000'' :: UUID';

CREATE FUNCTION prevent_change()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS
$$
BEGIN
  RAISE EXCEPTION 'TG_OP %: Records in table % cannot be altered.', lower(tg_op), tg_table_name;
END;
$$;

---------------------------------------------------------------
-- history tracking

-- audit.user is used to track who did the changes
-- SET "audit.user" TO 'bob@example.com';
-- RESET "audit.user";

-- Only the delta from current state is stored.
--   Inserts fully matched the current state, so entry will be an empty hstore
--   Updates will only record columns modified from current state
--   Deletes will track the entire entry as the current state becomes "nothing"
-- tx is the transaction changes occurred in so you can collate changes that occurred across multiple tables at the same time.
CREATE TABLE
  history (
            tx BIGINT,
            table_name VARCHAR NOT NULL,
            id UUID NOT NULL,
            who VARCHAR NOT NULL,
            tz TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), -- NOT now() or current_timestamp, we want the clock so a transaction that updates the same data twice won't hit a conflict on insert.
            op CHAR CHECK (op = ANY (ARRAY ['I' :: CHAR, 'U' :: CHAR, 'D' :: CHAR])),
            entry HSTORE,
            PRIMARY KEY (id, tz) -- table_name isn't required because tuids are globally unique, tz is required as the same id can be updated multiple times in one transaction
          );

-- NOTE: you may want to partition the history table by table_name

CREATE INDEX history_tx_id_tz ON history (tx, id, tz);
CREATE INDEX history_tn_id_tz ON history (table_name, id, tz);

CREATE TRIGGER history_prevent_change
  BEFORE UPDATE OR DELETE OR TRUNCATE
  ON history
EXECUTE PROCEDURE prevent_change();

CREATE OR REPLACE FUNCTION history_track_tg()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS
$X$
DECLARE
  who VARCHAR;
  tx BIGINT;
  newhs HSTORE;
  oldhs HSTORE;
  idname VARCHAR;
  id UUID;
BEGIN
  SELECT
    current_setting('audit.user')
  INTO who;

  IF who IS NULL OR who = ''
  THEN
    RAISE EXCEPTION 'audit.user is not set.';
  END IF;

  idname = tg_argv[0];

  tx = pg_current_xact_id ();

  IF tg_op = 'UPDATE'
  THEN
    oldhs = hstore(old);
    newhs = hstore(new);
    IF ((oldhs -> idname) != (newhs -> idname))
    THEN
      RAISE EXCEPTION 'id cannot be changed';
    END IF;

    id = (newhs -> idname) :: UUID;
    RAISE NOTICE '%', id;
    INSERT INTO history (id, table_name, tx, who, op, entry) VALUES (id, tg_table_name, tx, who, 'U', oldhs - newhs);
    RETURN new;
  END IF;

  IF tg_op = 'INSERT'
  THEN
    newhs = hstore(new);
    id = (newhs -> idname) :: UUID;
    RAISE NOTICE '%', id;
    INSERT INTO history (id, table_name, tx, who, op, entry) VALUES (id, tg_table_name, tx, who, 'I', ''::HSTORE);
    RETURN new;
  END IF;

  IF tg_op = 'DELETE'
  THEN
    oldhs = hstore(old);
    id = (oldhs -> idname) :: UUID;
    RAISE NOTICE '%', id;
    INSERT INTO history (id, table_name, tx, who, op, entry) VALUES (id, tg_table_name, tx, who, 'D', oldhs);
    RETURN old;
  END IF;

  RETURN NULL;
END;
$X$;

-- function to setup history table and triggers to prevent history alteration and tracking of changes
CREATE FUNCTION add_history_to_table(table_name VARCHAR, id_column_name VARCHAR = NULL)
  RETURNS VOID
  LANGUAGE plpgsql
AS
$$
BEGIN
  IF id_column_name IS NULL
  THEN
    id_column_name = table_name || '_id';
  END IF;

  -- hook up the trigger
  EXECUTE FORMAT(
    'CREATE TRIGGER %I
      BEFORE UPDATE OR DELETE OR INSERT
      ON %I
      FOR EACH ROW EXECUTE PROCEDURE history_track_tg(%L);
    ',
    table_name || '_history',
    table_name,
    id_column_name
    );
END;
$$;

-----------------------------------------------------------------------------------------------------
-- example usage

SET "audit.user" TO 'bob@example.com';

CREATE TABLE "user"
(
  user_id UUID DEFAULT tuid_generate() PRIMARY KEY,
  first_name VARCHAR NOT NULL,
  last_name VARCHAR NOT NULL,
  dob DATE,
  email VARCHAR NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT add_history_to_table('user');

INSERT INTO
  "user" (first_name, last_name, dob, email)
VALUES
  ('bob', 'smith', '1968-09-23', 'bob@example.com');

UPDATE "user"
SET
  first_name = 'robert'
WHERE
  email = 'bob@example.com';

DELETE
FROM
  "user"
WHERE
  email = 'bob@example.com';

SELECT *
FROM
  history;

RESET "audit.user";

ROLLBACK;

