create table t (i int);
desc t;
select last_query_id();
drop database if exists test_db;
begin;
create database test_db;
use test_db;
create table test_table(
col1 int,
col2 varchar
);
desc test_table;
select last_query_id();
/* cloud_user */ desc test_table;
select last_query_id();
/* cloud_nouser */ desc test_table;
select last_query_id();
commit;
