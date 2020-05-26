BEGIN;

CREATE SCHEMA IF NOT EXISTS eg;
SET SEARCH_PATH = eg;

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
DECLARE
  ct BIGINT;
  r BYTEA;
  r0 BIGINT;
  r1 BIGINT;
  r2 BIGINT;
  ax BIGINT;
  bx BIGINT;
  cx BIGINT;
  dx BIGINT;
  ret VARCHAR;
BEGIN
  r := gen_random_bytes(8); -- we use 58 bits of this

  r0 := (get_byte(r, 0) << 8) | get_byte(r, 1);
  r1 := (get_byte(r, 2) << 8) | get_byte(r, 3);

  -- The & mask here is to suppress the sign extension on the 32nd bit.
  r2 := ((get_byte(r, 4) << 24) | (get_byte(r, 5) << 16) | (get_byte(r, 6) << 8) | get_byte(r, 7)) & x'0FFFFFFFF'::BIGINT;

  ct := extract(EPOCH FROM clock_timestamp() AT TIME ZONE 'utc') * 1000000;

  ax := ct >> 32;
  bx := ct >> 16 & x'FFFF' :: INT;
  cx := x'4000' :: INT | ((ct >> 4) & x'0FFF' :: INT);
  dx := x'8000' :: INT | ((ct & x'F' :: INT) << 10) | ((r0 >> 6) & x'3F' :: INT);

  ret :=
    LPAD(TO_HEX(ax), 8, '0') ||
      LPAD(TO_HEX(bx), 4, '0') ||
      LPAD(TO_HEX(cx), 4, '0') ||
      LPAD(TO_HEX(dx), 4, '0') ||
      LPAD(TO_HEX(r1), 4, '0') ||
      LPAD(TO_HEX(r2), 8, '0');

  RETURN ret :: UUID;
END;
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
            rev UUID NOT NULL DEFAULT tuid_generate(),
            who VARCHAR NOT NULL,
            tz TIMESTAMPTZ NOT NULL DEFAULT now(),
            op CHAR CHECK (op = ANY (ARRAY ['I' :: CHAR, 'U' :: CHAR, 'D' :: CHAR])),
            entry HSTORE,
            PRIMARY KEY (table_name, id, rev)
          )
  PARTITION BY LIST (table_name);

CREATE INDEX history_tx_id_rev ON history (tx, id, rev);
CREATE INDEX history_tn_id_rev ON history (table_name, id, rev);

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

  tx = txid_current_if_assigned();
  IF tx IS NOT NULL
  THEN
    tx = TXID_SNAPSHOT_XMIN(txid_current_snapshot());
  ELSE
    tx = txid_current();
  END IF;

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

  -- create partition for table
  EXECUTE FORMAT('CREATE TABLE %I PARTITION OF history FOR VALUES IN (%L);', 'history_' || table_name, table_name);

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

SELECT *
FROM
  history_user;

RESET "audit.user";

ROLLBACK;