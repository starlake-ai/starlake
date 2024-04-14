CREATE SCHEMA IF NOT EXISTS starlake;
CREATE SCHEMA IF NOT EXISTS audit;

create table if not exists starlake.test_table
(
    id int,
    name string
);

insert into starlake.test_table values(1, 'a');
insert into starlake.test_table values(2, 'b');
insert into starlake.test_table values(3, 'c');
insert into starlake.test_table values(4, 'd');
insert into starlake.test_table values(5, 'e');
insert into starlake.test_table values(6, 'f');
insert into starlake.test_table values(7, 'g');
insert into starlake.test_table values(8, 'h');
insert into starlake.test_table values(9, 'i');
insert into starlake.test_table values(10, 'j');


