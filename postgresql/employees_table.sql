create table employees
(
    id         integer not null
        constraint employees_pk
            primary key,
    first_name varchar,
    last_name  varchar,
    salary     integer,
    department varchar
);