# 只读账号权限验证结果

## 测试环境

- **主机**: 127.0.0.1:6001
- **管理员用户**: dump
- **只读用户**: moi_readonly
- **测试数据库**: moi
- **测试表**: test_readonly
- **测试时间**: 2026-04-27

## 验证结果

### 1. ❌ SHOW ACCOUNTS - 权限被拒绝

```sql
SHOW ACCOUNTS;
```

**错误信息**:
```
ERROR 20101 (HY000): internal error: do not have privilege to execute the statement
```

**原因分析**:
根据代码 `pkg/frontend/authenticate.go`，`SHOW ACCOUNTS` 是一个**特殊权限**，只能由以下角色执行：
- `moadmin` (sys 租户的管理员角色)
- `accountadmin` (普通租户的管理员角色)

**代码证据**:
```go
checkShowAccountsPrivilege := func() (bool, error) {
    //only the moAdmin and accountAdmin can execute the show accounts.
    return tenant.IsAdminRole(), nil
}
```

**结论**: 
- ⚠️ **无法授予** - 这是管理员专属权限，无法授予给普通只读角色
- 如果需要查看账号信息，只读用户可以直接查询系统表：
  ```sql
  SELECT account_id, account_name, status, comments 
  FROM mo_catalog.mo_account;
  ```

---

### 2. ✅ mo_table_size() - 可以执行

```sql
SELECT mo_table_size('moi', 'test_readonly');
```

**执行结果**:
```
mo_table_size(moi, test_readonly)
216
```

**说明**:
- ✅ 函数正常执行
- ✅ 返回表大小: 216 字节
- ✅ 只读用户可以使用 `mo_table_size()` 函数查询表的大小

**结论**: 只读用户对已授权的表可以使用 `mo_table_size()` 函数

---

## 完整权限测试总结

### ✅ 可以执行的操作

| 操作 | 命令 | 结果 |
|------|------|------|
| 连接数据库 | `mysql -h... -umoi_readonly -p123` | ✅ 成功 |
| 查看数据库列表 | `SHOW DATABASES;` | ✅ 成功 |
| 使用数据库 | `USE moi;` | ✅ 成功 |
| 查看表列表 | `USE moi; SHOW TABLES;` | ✅ 成功 |
| 查询数据 | `SELECT * FROM moi.test_readonly;` | ✅ 成功 |
| 查询表大小 | `SELECT mo_table_size('moi', 'test_readonly');` | ✅ 成功 |
| 查询当前数据库 | `SELECT DATABASE();` | ✅ 成功 |
| 查看表结构 | `SELECT * FROM mo_catalog.mo_tables WHERE reldatabase='moi';` | ✅ 成功 |
| 查看列信息 | `SELECT * FROM mo_catalog.mo_columns WHERE att_database='moi';` | ✅ 成功 |

### ❌ 不能执行的操作

| 操作 | 命令 | 错误信息 |
|------|------|----------|
| 插入数据 | `INSERT INTO moi.test_readonly ...` | ❌ 权限拒绝 |
| 更新数据 | `UPDATE moi.test_readonly SET ...` | ❌ 权限拒绝 |
| 删除数据 | `DELETE FROM moi.test_readonly WHERE ...` | ❌ 权限拒绝 |
| 创建表 | `CREATE TABLE moi.new_table ...` | ❌ 权限拒绝 |
| 删除表 | `DROP TABLE moi.test_readonly;` | ❌ 权限拒绝 |
| 查看账号 | `SHOW ACCOUNTS;` | ❌ 权限拒绝（管理员专属） |
| 查看未授权表 | `SHOW TABLES FROM other_db;` | ❌ 权限拒绝 |
| 直接 SHOW TABLES FROM | `SHOW TABLES FROM moi;` | ❌ 权限拒绝（需先 USE） |

### ⚠️ 有限制的操作

| 操作 | 限制说明 | 解决方法 |
|------|----------|----------|
| `SHOW TABLES` | 不能使用 `SHOW TABLES FROM db` 语法 | 先执行 `USE db;` 再执行 `SHOW TABLES;` |
| 查询系统表 | 只能查询已授权的 mo_catalog 表 | 需要额外授权其他系统表 |

---

## 补充说明

### 关于 SHOW ACCOUNTS

虽然只读用户不能执行 `SHOW ACCOUNTS`，但可以通过查询系统表获取类似信息：

```sql
-- 查询账号信息（只读用户需要额外授权）
SELECT 
    account_id,
    account_name,
    status,
    created_time,
    comments
FROM mo_catalog.mo_account;
```

**如果需要授予此权限**，可以执行：

```sql
-- 使用管理员账号授予
GRANT SELECT ON TABLE mo_catalog.mo_account TO moi_readonly_role;
```

### 关于 mo_table_size()

`mo_table_size()` 函数属于系统函数，只读用户可以正常使用，前提是：
1. 用户对该表有 SELECT 权限
2. 用户可以访问该数据库

**其他有用的系统函数**：

```sql
-- 查看数据库大小
SELECT mo_ctl('cn', 'memused', '');

-- 查看表的行数（需要 SELECT 权限）
SELECT COUNT(*) FROM moi.test_readonly;

-- 查看表的详细信息
SELECT 
    relname AS table_name,
    reldatabase AS database_name,
    relcreatesql AS create_sql
FROM mo_catalog.mo_tables
WHERE reldatabase = 'moi' AND relname = 'test_readonly';
```

---

## 授权的角色和权限

### 当前角色: moi_readonly_role

```sql
SELECT 
    role_name,
    obj_type,
    privilege_name,
    privilege_level
FROM mo_catalog.mo_role_privs
WHERE role_name = 'moi_readonly_role'
ORDER BY obj_type, privilege_level;
```

**结果**:

| role_name | obj_type | privilege_name | privilege_level |
|-----------|----------|----------------|-----------------|
| moi_readonly_role | account | connect | * |
| moi_readonly_role | account | show databases | * |
| moi_readonly_role | database | show tables | d |
| moi_readonly_role | table | select | d.* |
| moi_readonly_role | table | select | d.t |
| moi_readonly_role | table | select | d.t |

**说明**:
- `account` 级别: 连接和显示数据库权限
- `database` 级别: 显示表权限（针对 moi 数据库）
- `table` 级别: SELECT 权限（moi.* 和 mo_catalog 的指定表）

---

## 安全性评估

### ✅ 安全特性

1. **读写分离**: 只读用户无法执行任何写操作（INSERT/UPDATE/DELETE/CREATE/DROP）
2. **权限隔离**: 无法访问未授权的数据库和表
3. **管理权限隔离**: 无法执行管理命令（SHOW ACCOUNTS）
4. **最小权限原则**: 只授予必要的 SELECT 权限

### ⚠️ 注意事项

1. **密码安全**: 示例中使用的密码 `123` 过于简单，生产环境应使用强密码
2. **权限范围**: 当前授权了 `moi.*` 所有表，如果需要更精细控制，应授予特定表权限
3. **系统表访问**: 当前只授权了 `mo_catalog.mo_tables` 和 `mo_catalog.mo_columns`，其他系统表无法访问

### 🔒 生产环境建议

1. **使用强密码**: 
   ```sql
   ALTER USER moi_readonly IDENTIFIED BY 'Str0ng!P@ssw0rd#2024';
   ```

2. **限制 IP 访问**: 在防火墙层面限制只读账号的访问来源

3. **定期审计**: 
   ```sql
   -- 查看只读用户的访问记录
   SELECT user, request_at, statement
   FROM system.statement_info
   WHERE user = 'moi_readonly'
   ORDER BY request_at DESC
   LIMIT 100;
   ```

4. **定期轮换密码**: 建议每 90 天更换一次密码

5. **监控异常访问**: 设置告警规则，监控只读账号的异常查询行为

---

## 结论

✅ 只读账号 `moi_readonly` 已成功创建，权限配置正确：
- ✅ 可以读取 moi 数据库的所有表
- ✅ 可以使用 `mo_table_size()` 等系统函数
- ✅ 无法执行任何写操作
- ❌ 无法执行管理命令（如 `SHOW ACCOUNTS`），这是设计预期
- ⚠️ `SHOW TABLES FROM db` 语法不支持，需要先 `USE db`

**总体评价**: 只读账号配置符合预期，满足只读访问需求，安全性良好。

---

## 相关文档

- [创建只读账号脚本](./create_readonly_account.sh)
- [脚本使用说明](./README_create_readonly_account.md)
- [sys 租户只读账号配置指南](../docs/handbooks/20260427-sys-tenant-readonly-account.md)

---

**测试完成时间**: 2026-04-27 10:19  
**测试人员**: Claude  
**测试状态**: ✅ 通过
