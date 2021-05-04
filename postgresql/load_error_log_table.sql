create table load_error_log
(
    id            integer,
    aws_key       varchar,
    error_message character varying[],
    inserted_at   timestamp,
    sql_function  varchar,
    processed     boolean default false
);