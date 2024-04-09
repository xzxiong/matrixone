-- @separator:table
explain SELECT * FROM
(SELECT CAST((JSON_UNQUOTE(JSON_EXTRACT(stats, '$[1]')) * 7.43e-14 + JSON_UNQUOTE(JSON_EXTRACT(stats, '$[2]')) * 6.79e-24 * duration + JSON_UNQUOTE(JSON_EXTRACT(stats, '$[3]')) * 1e-06 + JSON_UNQUOTE(JSON_EXTRACT(stats, '$[4]')) * 1e-06 + IF(JSON_UNQUOTE(JSON_EXTRACT(stats, '$[6]')) = 1, JSON_UNQUOTE(JSON_EXTRACT(stats, '$[5]')) * 0, JSON_UNQUOTE(JSON_EXTRACT(stats, '$[5]')) * 8.94e-10)) / 1.0026988039e-06 AS DECIMAL(32,2)) AS `cu`,
`statement`, statement_id, `duration`, `duration`/1e9 sec, `status` , `query_type` , `request_at` , system.statement_info.response_at,
`account`, `user` , `database` , `transaction_id` , `session_id` , `rows_read` , `bytes_scan` , `result_count`, `aggr_count`, `exec_plan`,
`sql_source_type`, `err_code`, `error`, `node_uuid`, `stats`
FROM system.statement_info
WHERE request_at BETWEEN FROM_UNIXTIME(1712599200) AND FROM_UNIXTIME(1712602799)
AND system.statement_info.account = "sys"
AND ("'ALL'" = "'ALL'" OR sql_source_type IN ('ALL'))
AND ("ALL" = "ALL" OR status = "ALL")
AND ("" = "" OR node_uuid = "")
AND ("" = "" OR session_id = "")
AND ("" = "" OR statement_id = "")
AND ("ALL"="ALL" OR query_type = "ALL")) t
ORDER BY  request_at DESC LIMIT 20;
select 1;
