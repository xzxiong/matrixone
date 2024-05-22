-- prepare
create account if not exists `bvt_query_tcp` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- START>
-- @session:id=1&user=bvt_query_tcp:admin:accountadmin&password=123456

-- case 1: select 8192 rows result
set @case_name="case:select_8192_rows";
create database if not exists test;
use test;
create table if not exists 32kb_8192row_int(a int);
-- rows: 16
insert into 32kb_8192row_int values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16);
-- rows: 32,64,128
insert into 32kb_8192row_int  select * from 32kb_8192row_int;
insert into 32kb_8192row_int  select * from 32kb_8192row_int;
insert into 32kb_8192row_int  select * from 32kb_8192row_int;
-- rows: 256->512->1,024
insert into 32kb_8192row_int  select * from 32kb_8192row_int;
insert into 32kb_8192row_int  select * from 32kb_8192row_int;
insert into 32kb_8192row_int  select * from 32kb_8192row_int;
-- rows: 2,048->4,096->8,192
insert into 32kb_8192row_int  select * from 32kb_8192row_int;
insert into 32kb_8192row_int  select * from 32kb_8192row_int;
insert into 32kb_8192row_int  select * from 32kb_8192row_int;

-- please ignore result
/*cloud_user*/select * from 32kb_8192row_int order by a;
-- END case 1


-- case 2
-- END case 2

-- case 3, load 1751 rows
set @case_name="case:load_1751_rows";
create database if not exists test;
use test;
drop table if exists rawlog_withnull;
CREATE  TABLE rawlog_withnull (
`raw_item` VARCHAR(1024),
`node_uuid` VARCHAR(36),
`node_type` VARCHAR(64),
`span_id` VARCHAR(16),
`statement_id` VARCHAR(36),
`logger_name` VARCHAR(1024),
`timestamp` DATETIME,
`level` VARCHAR(1024),
`caller` VARCHAR(1024),
`message` TEXT,
`extra` TEXT,
`err_code` VARCHAR(1024),
`error` TEXT,
`stack` VARCHAR(4096),
`span_name` VARCHAR(1024),
`parent_span_id` VARCHAR(16),
`start_time` DATETIME,
`end_time` DATETIME,
`duration` BIGINT UNSIGNED,
`resource` TEXT);
load data infile '$resources/external_table_file/rawlog_withnull.csv' into table rawlog_withnull fields terminated by ',' enclosed by '\"' lines terminated by '\n';

-- @session
-- END>

-- check sql: more in ../statement_query_type/query_tcp.sql
-- select statement,duration,stats,mo_cu(stats, duration, 'cpu') cpu, mo_cu(stats, duration, 'mem') mem, mo_cu(stats, duration, 'network') network from system.statement_info where statement_id in ('018e5a85-36ec-7649-b3f9-3ae73df2708d','018e5a86-2176-7b1e-ae45-b00aac9c7437');

-- clean
drop account `bvt_query_tcp`;
