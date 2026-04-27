# sys 租户只读账号配置指南

## 问题

**能否给 sys租户创建一个只读的账号，只有特定库表的读权限？**

## 答案

**可以**。MatrixOne 完全支持在 sys 租户中创建只读账号，并且可以精确控制到特定库表的读权限。

---

## 方案概述

在 MatrixOne 中，sys 租户是系统租户（account_id = 0），拥有对集群表的完全控制权限。但同样可以在 sys 租户中创建具有受限权限的用户和角色。

### 核心原理

1. **角色权限控制**: 通过创建角色并只授予 SELECT 权限
2. **精确的权限粒度**: 支持库级（database.*）和表级（database.table）权限
3. **权限隔离**: 即使在 sys 租户中，用户权限也受到角色权限的约束

---

## 完整实施步骤

### 步骤 1: 连接到 sys 租户

```bash
# 使用 root 用户（moadmin角色）连接 sys 租户
mysql -h <host> -P <port> -u root -p
```

### 步骤 2: 创建只读角色

```sql
-- 创建专用的只读角色
CREATE ROLE sys_readonly_role COMMENT 'Read-only role for sys tenant';
```

### 步骤 3: 授予基础权限

```sql
-- 授予连接权限（必需）
GRANT CONNECT ON ACCOUNT * TO sys_readonly_role;

-- 授予显示数据库列表的权限（可选，方便用户浏览）
GRANT SHOW DATABASES ON ACCOUNT * TO sys_readonly_role;

-- 授予显示表列表的权限（可选，方便用户浏览）
GRANT SHOW TABLES ON *.* TO sys_readonly_role;
```

### 步骤 4: 授予特定库表的 SELECT 权限

#### 方案 A: 授予整个数据库的只读权限

```sql
-- 授予某个数据库所有表的SELECT权限
GRANT SELECT ON my_database.* TO sys_readonly_role;

-- 如果有多个数据库
GRANT SELECT ON database1.* TO sys_readonly_role;
GRANT SELECT ON database2.* TO sys_readonly_role;
```

#### 方案 B: 授予特定表的只读权限

```sql
-- 只授予特定表的SELECT权限
GRANT SELECT ON mo_catalog.mo_tables TO sys_readonly_role;
GRANT SELECT ON mo_catalog.mo_database TO sys_readonly_role;
GRANT SELECT ON mo_catalog.mo_columns TO sys_readonly_role;

-- 系统监控表
GRANT SELECT ON system.statement_info TO sys_readonly_role;
GRANT SELECT ON system.metric TO sys_readonly_role;

-- 业务数据表
GRANT SELECT ON my_database.table1 TO sys_readonly_role;
GRANT SELECT ON my_database.table2 TO sys_readonly_role;
```

#### 方案 C: 混合方式（推荐）

```sql
-- 对于系统表，授予必要的几张表的权限
GRANT SELECT ON mo_catalog.mo_tables TO sys_readonly_role;
GRANT SELECT ON mo_catalog.mo_database TO sys_readonly_role;
GRANT SELECT ON mo_catalog.mo_columns TO sys_readonly_role;
GRANT SELECT ON mo_catalog.mo_account TO sys_readonly_role;

-- 对于业务数据库，授予整库权限
GRANT SELECT ON business_db.* TO sys_readonly_role;

-- 对于监控数据，授予特定表权限
GRANT SELECT ON system.statement_info TO sys_readonly_role;
```

### 步骤 5: 创建只读用户

```sql
-- 创建只读用户，并设置默认角色为只读角色
CREATE USER sys_readonly_user 
    IDENTIFIED BY 'SecurePassword123!' 
    DEFAULT ROLE sys_readonly_role
    COMMENT 'Read-only user for monitoring and reporting';
```

### 步骤 6: 授予角色给用户

```sql
-- 将只读角色授予用户
GRANT sys_readonly_role TO sys_readonly_user;
```

### 步骤 7: 验证配置

```sql
-- 查看用户的角色
SELECT 
    u.user_name,
    r.role_name,
    ug.with_grant_option,
    ug.granted_time
FROM mo_catalog.mo_user u
JOIN mo_catalog.mo_user_grant ug ON u.user_id = ug.user_id
JOIN mo_catalog.mo_role r ON ug.role_id = r.role_id
WHERE u.user_name = 'sys_readonly_user';

-- 查看角色的所有权限
SELECT 
    role_name,
    obj_type,
    privilege_name,
    privilege_level,
    with_grant_option,
    granted_time
FROM mo_catalog.mo_role_privs
WHERE role_name = 'sys_readonly_role'
ORDER BY obj_type, privilege_level, privilege_name;
```

---

## 完整示例脚本

### 示例 1: 创建 mo_catalog 只读监控账号

```sql
-- ============================================
-- 创建 mo_catalog 只读监控账号
-- ============================================

-- 1. 创建角色
CREATE ROLE sys_catalog_readonly COMMENT 'Read-only access to mo_catalog tables';

-- 2. 授予基础权限
GRANT CONNECT ON ACCOUNT * TO sys_catalog_readonly;
GRANT SHOW DATABASES ON ACCOUNT * TO sys_catalog_readonly;
GRANT SHOW TABLES ON DATABASE mo_catalog TO sys_catalog_readonly;

-- 3. 授予 mo_catalog 核心表的 SELECT 权限
GRANT SELECT ON mo_catalog.mo_tables TO sys_catalog_readonly;
GRANT SELECT ON mo_catalog.mo_database TO sys_catalog_readonly;
GRANT SELECT ON mo_catalog.mo_columns TO sys_catalog_readonly;
GRANT SELECT ON mo_catalog.mo_user TO sys_catalog_readonly;
GRANT SELECT ON mo_catalog.mo_role TO sys_catalog_readonly;
GRANT SELECT ON mo_catalog.mo_role_privs TO sys_catalog_readonly;
GRANT SELECT ON mo_catalog.mo_user_grant TO sys_catalog_readonly;
GRANT SELECT ON mo_catalog.mo_role_grant TO sys_catalog_readonly;
GRANT SELECT ON mo_catalog.mo_account TO sys_catalog_readonly;

-- 4. 创建用户
CREATE USER catalog_monitor 
    IDENTIFIED BY 'Monitor@2024!' 
    DEFAULT ROLE sys_catalog_readonly
    COMMENT 'Monitoring user for catalog tables';

-- 5. 授予角色
GRANT sys_catalog_readonly TO catalog_monitor;

-- 6. 验证
SELECT 
    rp.privilege_name,
    rp.privilege_level,
    rp.obj_type
FROM mo_catalog.mo_role_privs rp
WHERE rp.role_name = 'sys_catalog_readonly'
ORDER BY rp.privilege_level;
```

### 示例 2: 创建系统监控只读账号

```sql
-- ============================================
-- 创建系统监控只读账号
-- ============================================

-- 1. 创建角色
CREATE ROLE sys_monitor_readonly COMMENT 'System monitoring read-only access';

-- 2. 授予基础权限
GRANT CONNECT ON ACCOUNT * TO sys_monitor_readonly;
GRANT SHOW DATABASES ON ACCOUNT * TO sys_monitor_readonly;
GRANT SHOW TABLES ON *.* TO sys_monitor_readonly;

-- 3. 授予系统监控表权限
GRANT SELECT ON system.statement_info TO sys_monitor_readonly;
GRANT SELECT ON system.metric TO sys_monitor_readonly;
GRANT SELECT ON system.log_info TO sys_monitor_readonly;

-- 4. 授予必要的 mo_catalog 权限
GRANT SELECT ON mo_catalog.mo_account TO sys_monitor_readonly;
GRANT SELECT ON mo_catalog.mo_user TO sys_monitor_readonly;

-- 5. 创建用户
CREATE USER system_monitor 
    IDENTIFIED BY 'SysMonitor@2024!' 
    DEFAULT ROLE sys_monitor_readonly
    COMMENT 'System monitoring and metrics collection user';

-- 6. 授予角色
GRANT sys_monitor_readonly TO system_monitor;
```

### 示例 3: 创建业务数据只读分析账号

```sql
-- ============================================
-- 创建业务数据只读分析账号
-- ============================================

-- 1. 创建角色
CREATE ROLE sys_analyst_readonly COMMENT 'Business data analysis read-only access';

-- 2. 授予基础权限
GRANT CONNECT ON ACCOUNT * TO sys_analyst_readonly;
GRANT SHOW DATABASES ON ACCOUNT * TO sys_analyst_readonly;
GRANT SHOW TABLES ON DATABASE analytics_db TO sys_analyst_readonly;
GRANT SHOW TABLES ON DATABASE reports_db TO sys_analyst_readonly;

-- 3. 授予特定业务库的只读权限
GRANT SELECT ON analytics_db.* TO sys_analyst_readonly;
GRANT SELECT ON reports_db.* TO sys_analyst_readonly;

-- 4. 授予特定系统表的权限（用于元数据查询）
GRANT SELECT ON mo_catalog.mo_tables TO sys_analyst_readonly;
GRANT SELECT ON mo_catalog.mo_columns TO sys_analyst_readonly;

-- 5. 创建用户
CREATE USER data_analyst 
    IDENTIFIED BY 'Analyst@2024!' 
    DEFAULT ROLE sys_analyst_readonly
    COMMENT 'Data analyst with read-only access';

-- 6. 授予角色
GRANT sys_analyst_readonly TO data_analyst;
```

---

## 测试和验证

### 测试 1: 使用只读账号连接

```bash
# 使用新创建的只读用户连接
mysql -h <host> -P <port> -u sys_readonly_user -p
```

### 测试 2: 验证读权限

```sql
-- 应该成功：查询授权的表
SELECT COUNT(*) FROM mo_catalog.mo_tables;
SELECT * FROM my_database.my_table LIMIT 10;

-- 应该成功：显示数据库和表
SHOW DATABASES;
SHOW TABLES FROM mo_catalog;
```

### 测试 3: 验证写权限被禁止

```sql
-- 应该失败：尝试插入数据
INSERT INTO my_database.my_table (col1) VALUES ('test');
-- 错误: Access denied for user 'sys_readonly_user'

-- 应该失败：尝试更新数据
UPDATE my_database.my_table SET col1 = 'test' WHERE id = 1;
-- 错误: Access denied for user 'sys_readonly_user'

-- 应该失败：尝试删除数据
DELETE FROM my_database.my_table WHERE id = 1;
-- 错误: Access denied for user 'sys_readonly_user'

-- 应该失败：尝试创建表
CREATE TABLE my_database.new_table (id INT);
-- 错误: Access denied for user 'sys_readonly_user'

-- 应该失败：尝试删除表
DROP TABLE my_database.my_table;
-- 错误: Access denied for user 'sys_readonly_user'
```

### 测试 4: 验证未授权表的访问被拒绝

```sql
-- 应该失败：访问未授权的表
SELECT * FROM unauthorized_db.some_table;
-- 错误: Access denied for user 'sys_readonly_user'
```

---

## 权限管理最佳实践

### 1. 最小权限原则

```sql
-- ❌ 避免：授予过多权限
GRANT SELECT ON *.* TO readonly_role;

-- ✅ 推荐：只授予需要的库表权限
GRANT SELECT ON specific_db.table1 TO readonly_role;
GRANT SELECT ON specific_db.table2 TO readonly_role;
```

### 2. 使用角色而非直接授权

```sql
-- ❌ 避免：直接授权给用户
GRANT SELECT ON db.table TO user1;

-- ✅ 推荐：通过角色授权
CREATE ROLE readonly_role;
GRANT SELECT ON db.table TO readonly_role;
GRANT readonly_role TO user1;
```

### 3. 定期审计权限

```sql
-- 审计只读角色的权限
SELECT 
    r.role_name,
    rp.obj_type,
    rp.privilege_name,
    rp.privilege_level,
    COUNT(*) as privilege_count
FROM mo_catalog.mo_role r
JOIN mo_catalog.mo_role_privs rp ON r.role_id = rp.role_id
WHERE r.role_name LIKE '%readonly%'
GROUP BY r.role_name, rp.obj_type, rp.privilege_name, rp.privilege_level
ORDER BY r.role_name, rp.obj_type;

-- 审计具有只读角色的用户
SELECT 
    u.user_name,
    u.status,
    u.created_time,
    u.login_type,
    r.role_name
FROM mo_catalog.mo_user u
JOIN mo_catalog.mo_user_grant ug ON u.user_id = ug.user_id
JOIN mo_catalog.mo_role r ON ug.role_id = r.role_id
WHERE r.role_name LIKE '%readonly%'
ORDER BY u.user_name;
```

### 4. 监控只读账号的访问

```sql
-- 查询只读用户的访问记录（如果启用了审计日志）
SELECT 
    user,
    request_at,
    statement,
    exec_plan
FROM system.statement_info
WHERE user = 'sys_readonly_user'
  AND request_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)
ORDER BY request_at DESC
LIMIT 100;
```

---

## 权限修改和维护

### 添加新表的权限

```sql
-- 当需要授予新表的权限时
GRANT SELECT ON new_database.new_table TO sys_readonly_role;
```

### 撤销特定表的权限

```sql
-- 当需要撤销某个表的权限时
REVOKE SELECT ON database.table FROM sys_readonly_role;
```

### 扩展权限到整个数据库

```sql
-- 从表级权限升级到库级权限
-- 先撤销所有表级权限
REVOKE SELECT ON db.table1 FROM sys_readonly_role;
REVOKE SELECT ON db.table2 FROM sys_readonly_role;

-- 授予整库权限
GRANT SELECT ON db.* TO sys_readonly_role;
```

### 临时授予额外权限

```sql
-- 创建临时角色（例如临时需要导出权限）
CREATE ROLE temp_export_role;
GRANT SELECT ON export_db.* TO temp_export_role;

-- 临时授予用户
GRANT temp_export_role TO sys_readonly_user;

-- 任务完成后撤销
REVOKE temp_export_role FROM sys_readonly_user;
DROP ROLE temp_export_role;
```

---

## 常见问题排查

### 问题 1: 用户无法登录

**症状**: 用户连接被拒绝

**检查步骤**:
```sql
-- 1. 检查用户是否存在
SELECT user_id, user_name, status, login_type 
FROM mo_catalog.mo_user 
WHERE user_name = 'sys_readonly_user';

-- 2. 检查用户状态
-- status应该是'unlock'，而不是'lock'或'forbid'

-- 3. 检查是否有CONNECT权限
SELECT rp.* 
FROM mo_catalog.mo_role_privs rp
JOIN mo_catalog.mo_user_grant ug ON rp.role_id = ug.role_id
WHERE ug.user_id = (SELECT user_id FROM mo_catalog.mo_user WHERE user_name = 'sys_readonly_user')
  AND rp.privilege_name = 'CONNECT';
```

**解决方案**:
```sql
-- 解锁用户
ALTER USER sys_readonly_user UNLOCK;

-- 确保有CONNECT权限
GRANT CONNECT ON ACCOUNT * TO sys_readonly_role;
```

### 问题 2: 无法查看数据库列表

**症状**: 执行 `SHOW DATABASES` 返回空或错误

**解决方案**:
```sql
-- 授予SHOW DATABASES权限
GRANT SHOW DATABASES ON ACCOUNT * TO sys_readonly_role;
```

### 问题 3: 无法查看表列表

**症状**: 执行 `SHOW TABLES` 返回空或错误

**解决方案**:
```sql
-- 授予SHOW TABLES权限
GRANT SHOW TABLES ON DATABASE database_name TO sys_readonly_role;
-- 或者
GRANT SHOW TABLES ON *.* TO sys_readonly_role;
```

### 问题 4: SELECT权限不生效

**症状**: 明明授予了SELECT权限，但查询仍然被拒绝

**检查步骤**:
```sql
-- 1. 验证权限是否正确授予
SELECT * FROM mo_catalog.mo_role_privs
WHERE role_name = 'sys_readonly_role'
  AND privilege_name = 'SELECT';

-- 2. 验证用户是否关联了角色
SELECT r.role_name
FROM mo_catalog.mo_user_grant ug
JOIN mo_catalog.mo_role r ON ug.role_id = r.role_id
WHERE ug.user_id = (SELECT user_id FROM mo_catalog.mo_user WHERE user_name = 'sys_readonly_user');

-- 3. 检查权限级别是否匹配
-- 如果授予的是 'db.table'，确保查询的是正确的数据库和表
```

**解决方案**:
```sql
-- 重新授予权限
GRANT SELECT ON database.table TO sys_readonly_role;

-- 确保用户关联了角色
GRANT sys_readonly_role TO sys_readonly_user;

-- 用户需要重新连接以刷新权限缓存
```

### 问题 5: 权限变更不生效

**症状**: 修改权限后，用户权限没有变化

**解决方案**:
1. 用户需要**断开并重新连接**数据库（权限缓存在会话中）
2. 权限更改立即生效，但需要新会话才能看到

---

## sys 租户特殊说明

### sys 租户的特权

- sys 租户（account_id = 0）是系统租户
- 对 `mo_catalog` 数据库的集群表有完全控制权
- 即使是只读用户，也会受到角色权限的限制

**代码位置**: `pkg/frontend/authenticate2.go:26-44`

```go
// verifyAccountCanOperateClusterTable 决定账户是否可以操作集群表
func verifyAccountCanOperateClusterTable(account *TenantInfo,
    dbName string,
    clusterTableOperation clusterTableOperationType) bool {
    if account.IsSysTenant() {
        // sys租户可以对mo_catalog做任何操作
        if dbName == moCatalog {
            return true
        }
    } else {
        // 普通租户只能读取mo_catalog的集群表
        if dbName == moCatalog {
            switch clusterTableOperation {
            case clusterTableNone, clusterTableSelect:
                return true
            }
        }
    }
    return false
}
```

### 重要提示

即使在 sys 租户中创建了只读用户：
- ✅ 只读用户受到角色权限限制，只能SELECT授权的表
- ✅ 无法执行INSERT、UPDATE、DELETE、DROP等写操作
- ✅ 无法访问未授权的表和数据库
- ⚠️ root用户（moadmin角色）仍然拥有所有权限

---

## 生产环境部署建议

### 1. 密码策略

```sql
-- 使用强密码
CREATE USER sys_readonly_user 
    IDENTIFIED BY 'Str0ng!P@ssw0rd#2024' 
    DEFAULT ROLE sys_readonly_role;

-- 定期更换密码
ALTER USER sys_readonly_user IDENTIFIED BY 'NewStr0ng!P@ssw0rd#2024';
```

### 2. 访问控制

```sql
-- 限制用户的主机访问（如果支持）
-- CREATE USER sys_readonly_user@'10.0.0.%' IDENTIFIED BY 'password';

-- 创建不同用途的只读角色
CREATE ROLE monitoring_readonly;   -- 监控用
CREATE ROLE reporting_readonly;    -- 报表用
CREATE ROLE analytics_readonly;    -- 分析用
```

### 3. 审计和监控

```sql
-- 启用审计日志
-- 监控只读账号的查询活动
-- 设置告警规则：如果只读账号尝试执行写操作

-- 定期审查权限
SELECT 
    u.user_name,
    r.role_name,
    rp.privilege_name,
    rp.privilege_level,
    rp.granted_time
FROM mo_catalog.mo_user u
JOIN mo_catalog.mo_user_grant ug ON u.user_id = ug.user_id
JOIN mo_catalog.mo_role r ON ug.role_id = r.role_id
JOIN mo_catalog.mo_role_privs rp ON r.role_id = rp.role_id
WHERE u.user_name LIKE '%readonly%'
ORDER BY u.user_name, rp.privilege_level;
```

### 4. 文档化

为每个只读账号维护文档：
- 创建目的和用途
- 授权的库表列表
- 负责人和联系方式
- 创建日期和审查周期
- 权限变更历史

---

## 总结

### ✅ 可以实现的功能

1. **在 sys 租户创建只读账号** - 完全支持
2. **限制到特定库表** - 精确到表级别
3. **禁止写操作** - 只授予 SELECT 权限
4. **灵活的权限管理** - 通过角色管理
5. **权限缓存优化** - 高性能权限检查

### 📋 实施清单

- [ ] 创建只读角色
- [ ] 授予 CONNECT 权限
- [ ] 授予 SHOW DATABASES/TABLES 权限（可选）
- [ ] 授予特定库表的 SELECT 权限
- [ ] 创建只读用户
- [ ] 关联角色到用户
- [ ] 测试读权限
- [ ] 测试写权限被禁止
- [ ] 测试未授权表访问被拒绝
- [ ] 文档化账号信息
- [ ] 设置监控和审计

### 🔗 相关文档

- [权限管理手册](./privilege-management.md)
- 代码位置: `pkg/frontend/authenticate.go`
- 代码位置: `pkg/frontend/authenticate2.go`
- 系统表定义: `pkg/frontend/predefined.go`

---

**文档版本**: v1.0  
**最后更新**: 2026-04-27  
**适用版本**: MatrixOne 3.0+
