-- CREATE USER hiveuser WITH PASSWORD 'hivepassword';
-- CREATE DATABASE metastore;
GRANT ALL PRIVILEGES ON DATABASE metastore TO hiveuser;


CREATE USER airflow WITH PASSWORD 'airflow' SUPERUSER;
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

CREATE USER superset WITH PASSWORD 'superset' SUPERUSER;
CREATE DATABASE superset OWNER superset;
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;


-- metabase descontinuado.
-- CREATE USER metabase WITH PASSWORD 'metabase';
-- CREATE DATABASE metabase OWNER metabase;
-- GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase;