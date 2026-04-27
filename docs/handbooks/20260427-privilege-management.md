# MatrixOne 权限管理手册

## 目录

- [1. 概述](#1-概述)
- [2. 权限管理体系结构](#2-权限管理体系结构)
- [3. 用户和角色](#3-用户和角色)
- [4. 权限类型](#4-权限类型)
- [5. 权限级别](#5-权限级别)
- [6. 系统表结构](#6-系统表结构)
- [7. 权限授予与撤销](#7-权限授予与撤销)
- [8. 系统租户权限机制](#8-系统租户权限机制)
- [9. 权限检查流程](#9-权限检查流程)
- [10. 只读权限配置](#10-只读权限配置)
- [11. 常见场景示例](#11-常见场景示例)

---

## 1. 概述

MatrixOne 采用基于角色的访问控制（RBAC）模型，提供细粒度的权限管理机制。权限管理体系分为多个层次：

- **系统租户 (sys)**: 最高级别，管理整个集群
- **普通租户 (account)**: 租户级别，管理租户内的资源
- **用户 (user)**: 登录用户
- **角色 (role)**: 权限集合，可分配给用户
- **数据库 (database)**: 数据库级别权限
- **表 (table)**: 表级别权限

---

## 2. 权限管理体系结构

### 2.1 层次结构

```
系统租户 (sys)
  └─ 账户 (account)
      ├─ 用户 (user)
      │   └─ 角色 (role)
      ├─ 数据库 (database)
      │   └─ 表 (table)
      └─ 其他对象 (function, view, etc.)
```

### 2.2 核心组件

**代码位置**: `pkg/frontend/authenticate.go`, `pkg/frontend/authenticate2.go`

#### 权限缓存机制
- 使用 btree 数据结构实现高效权限缓存
- 支持按账户、数据库、表的多维度缓存
- 缓存包含账户级、数据库级、表级权限
- 自动失效机制，权限变更时清除相关缓存

#### 权限检查流程
1. **轻量级检查** (`verifyLightPrivilege`): 不访问权限表的快速检查
2. **缓存检查** (`checkPrivilegeInCache`): 从缓存中查询权限
3. **完整检查** (`determineUserHasPrivilegeSet`): 访问系统表进行完整验证

---

## 3. 用户和角色

### 3.1 系统预定义角色

**代码位置**: `pkg/frontend/authenticate.go:507-568`

| 角色名 | 角色ID | 描述 | 权限范围 |
|--------|--------|------|----------|
| `moadmin` | 0 | 超级管理员角色 | 全部权限（34项），包括集群管理 |
| `accountadmin` | 2 | 账户管理员角色 | 租户内全部权限（30项），不含系统级操作 |
| `public` | 1 | 公共角色 | 仅CONNECT权限 |

**代码位置**: `pkg/frontend/authenticate.go:2267-2342`

#### moadmin 角色权限列表 (34项)
```
CREATE_ACCOUNT, DROP_ACCOUNT, ALTER_ACCOUNT, UPGRADE_ACCOUNT,
CREATE_USER, DROP_USER, ALTER_USER,
CREATE_ROLE, DROP_ROLE,
CREATE_DATABASE, DROP_DATABASE, SHOW_DATABASES,
CONNECT, MANAGE_GRANTS, ACCOUNT_ALL,
SHOW_TABLES, CREATE_TABLE, DROP_TABLE, ALTER_TABLE,
CREATE_VIEW, DROP_VIEW, ALTER_VIEW,
DATABASE_ALL, DATABASE_OWNERSHIP,
SELECT, INSERT, UPDATE, TRUNCATE, DELETE,
REFERENCE, INDEX,
TABLE_ALL, TABLE_OWNERSHIP, VALUES
```

#### accountadmin 角色权限列表 (30项)
与 moadmin 相比，不包含以下4项系统级权限：
- `CREATE_ACCOUNT`
- `DROP_ACCOUNT`
- `ALTER_ACCOUNT`
- `UPGRADE_ACCOUNT`

### 3.2 系统用户

**代码位置**: `pkg/frontend/authenticate.go:534-562`

| 用户名 | 用户ID | 默认角色 | 描述 |
|--------|--------|----------|------|
| `root` | 0 | moadmin | 系统根用户 |
| `dump` | 1 | moadmin | 备份导出用户 |

### 3.3 系统表定义

**代码位置**: `pkg/frontend/predefined.go:27-96`

#### mo_catalog.mo_user (用户表)
```sql
CREATE TABLE mo_catalog.mo_user (
    user_id INT SIGNED AUTO_INCREMENT PRIMARY KEY,
    user_host VARCHAR(100),
    user_name VARCHAR(300) UNIQUE KEY,
    authentication_string VARCHAR(100),
    status VARCHAR(8),  -- 'unlock', 'lock', 'forbid'
    created_time TIMESTAMP,
    expired_time TIMESTAMP,
    password_last_changed TIMESTAMP DEFAULT UTC_TIMESTAMP,
    password_history TEXT DEFAULT '[]',
    login_attempts INT UNSIGNED DEFAULT 0,
    lock_time TIMESTAMP DEFAULT UTC_TIMESTAMP,
    login_type VARCHAR(16),
    creator INT SIGNED,
    owner INT SIGNED,
    default_role INT SIGNED
);
```

#### mo_catalog.mo_role (角色表)
```sql
CREATE TABLE mo_catalog.mo_role (
    role_id INT SIGNED AUTO_INCREMENT PRIMARY KEY,
    role_name VARCHAR(300) UNIQUE KEY,
    creator INT SIGNED,
    owner INT SIGNED,
    created_time TIMESTAMP,
    comments TEXT
);
```

#### mo_catalog.mo_user_grant (用户角色授权表)
```sql
CREATE TABLE mo_catalog.mo_user_grant (
    role_id INT SIGNED,
    user_id INT SIGNED,
    granted_time TIMESTAMP,
    with_grant_option BOOL,
    PRIMARY KEY(role_id, user_id)
);
```

#### mo_catalog.mo_role_grant (角色继承表)
```sql
CREATE TABLE mo_catalog.mo_role_grant (
    granted_id INT SIGNED,        -- 被授予的角色ID
    grantee_id INT SIGNED,        -- 接收者角色ID
    operation_role_id INT SIGNED, -- 执行操作的角色ID
    operation_user_id INT SIGNED, -- 执行操作的用户ID
    granted_time TIMESTAMP,
    with_grant_option BOOL,
    PRIMARY KEY(granted_id, grantee_id)
);
```

#### mo_catalog.mo_role_privs (角色权限表)
```sql
CREATE TABLE mo_catalog.mo_role_privs (
    role_id INT SIGNED,
    role_name VARCHAR(100),
    obj_type VARCHAR(16),         -- 'account', 'database', 'table', 'function'
    obj_id BIGINT UNSIGNED,
    privilege_id INT,
    privilege_name VARCHAR(100),
    privilege_level VARCHAR(100), -- '*', '*.*', 'db.*', 'db.table'
    operation_user_id INT UNSIGNED,
    granted_time TIMESTAMP,
    with_grant_option BOOL,
    PRIMARY KEY(role_id, obj_type, obj_id, privilege_id, privilege_level)
);
```

---

## 4. 权限类型

**代码位置**: `pkg/frontend/authenticate.go:639-685`

### 4.1 账户级权限 (Account Level)

| 权限类型 | 枚举常量 | 描述 |
|---------|---------|------|
| CREATE ACCOUNT | `PrivilegeTypeCreateAccount` | 创建租户账户 |
| DROP ACCOUNT | `PrivilegeTypeDropAccount` | 删除租户账户 |
| ALTER ACCOUNT | `PrivilegeTypeAlterAccount` | 修改租户账户 |
| UPGRADE ACCOUNT | `PrivilegeTypeUpgradeAccount` | 升级租户账户 |
| CREATE USER | `PrivilegeTypeCreateUser` | 创建用户 |
| DROP USER | `PrivilegeTypeDropUser` | 删除用户 |
| ALTER USER | `PrivilegeTypeAlterUser` | 修改用户 |
| CREATE ROLE | `PrivilegeTypeCreateRole` | 创建角色 |
| DROP ROLE | `PrivilegeTypeDropRole` | 删除角色 |
| ALTER ROLE | `PrivilegeTypeAlterRole` | 修改角色 |
| SHOW DATABASES | `PrivilegeTypeShowDatabases` | 显示数据库列表 |
| CONNECT | `PrivilegeTypeConnect` | 连接数据库 |
| MANAGE GRANTS | `PrivilegeTypeManageGrants` | 管理权限授予 |
| ACCOUNT ALL | `PrivilegeTypeAccountAll` | 账户级所有权限 |
| ACCOUNT OWNERSHIP | `PrivilegeTypeAccountOwnership` | 账户所有权 |
| USER OWNERSHIP | `PrivilegeTypeUserOwnership` | 用户所有权 |
| ROLE OWNERSHIP | `PrivilegeTypeRoleOwnership` | 角色所有权 |

### 4.2 数据库级权限 (Database Level)

| 权限类型 | 枚举常量 | 描述 |
|---------|---------|------|
| CREATE DATABASE | `PrivilegeTypeCreateDatabase` | 创建数据库 |
| DROP DATABASE | `PrivilegeTypeDropDatabase` | 删除数据库 |
| SHOW TABLES | `PrivilegeTypeShowTables` | 显示表列表 |
| CREATE OBJECT | `PrivilegeTypeCreateObject` | 创建对象（表、视图等） |
| CREATE TABLE | `PrivilegeTypeCreateTable` | 创建表 |
| CREATE VIEW | `PrivilegeTypeCreateView` | 创建视图 |
| DROP OBJECT | `PrivilegeTypeDropObject` | 删除对象 |
| DROP TABLE | `PrivilegeTypeDropTable` | 删除表 |
| DROP VIEW | `PrivilegeTypeDropView` | 删除视图 |
| ALTER OBJECT | `PrivilegeTypeAlterObject` | 修改对象 |
| ALTER TABLE | `PrivilegeTypeAlterTable` | 修改表结构 |
| ALTER VIEW | `PrivilegeTypeAlterView` | 修改视图 |
| DATABASE ALL | `PrivilegeTypeDatabaseAll` | 数据库级所有权限 |
| DATABASE OWNERSHIP | `PrivilegeTypeDatabaseOwnership` | 数据库所有权 |

### 4.3 表级权限 (Table Level)

| 权限类型 | 枚举常量 | 描述 |
|---------|---------|------|
| SELECT | `PrivilegeTypeSelect` | 查询数据 |
| INSERT | `PrivilegeTypeInsert` | 插入数据 |
| UPDATE | `PrivilegeTypeUpdate` | 更新数据 |
| DELETE | `PrivilegeTypeDelete` | 删除数据 |
| TRUNCATE | `PrivilegeTypeTruncate` | 清空表 |
| REFERENCE | `PrivilegeTypeReference` | 引用（外键） |
| INDEX | `PrivilegeTypeIndex` | 创建/删除索引 |
| TABLE ALL | `PrivilegeTypeTableAll` | 表级所有权限 |
| TABLE OWNERSHIP | `PrivilegeTypeTableOwnership` | 表所有权 |
| EXECUTE | `PrivilegeTypeExecute` | 执行存储过程/函数 |
| VALUES | `PrivilegeTypeValues` | VALUES 子句权限 |

---

## 5. 权限级别

**代码位置**: `pkg/frontend/authenticate.go:598-637`

MatrixOne 支持7种权限级别，从全局到具体对象：

| 权限级别类型 | 枚举常量 | 语法示例 | 描述 |
|------------|---------|---------|------|
| `*` | `privilegeLevelStar` | `GRANT ... ON * TO role` | 当前数据库所有表 |
| `*.*` | `privilegeLevelStarStar` | `GRANT ... ON *.* TO role` | 全局所有数据库所有表 |
| `database` | `privilegeLevelDatabase` | `GRANT ... ON DATABASE db TO role` | 特定数据库本身 |
| `database.*` | `privilegeLevelDatabaseStar` | `GRANT ... ON db.* TO role` | 特定数据库的所有表 |
| `database.table` | `privilegeLevelDatabaseTable` | `GRANT ... ON db.table TO role` | 特定数据库的特定表 |
| `table` | `privilegeLevelTable` | `GRANT ... ON TABLE table TO role` | 当前数据库的特定表 |
| `routine` | `privilegeLevelRoutine` | `GRANT ... ON routine TO role` | 存储过程/函数 |

**代码位置**: `pkg/sql/parsers/tree/revoke.go:171-184`

---

## 6. 系统表结构

### 6.1 权限相关系统表关系图

```
mo_account (账户表)
    ↓ (account_id)
mo_user (用户表)
    ↓ (user_id)
mo_user_grant (用户-角色关联表)
    ↓ (role_id)
mo_role (角色表)
    ↓ (role_id)
mo_role_grant (角色继承表)
    ↓ (granted_id)
mo_role_privs (角色权限表)
```

### 6.2 对象类型 (Object Type)

**代码位置**: `pkg/frontend/authenticate.go:570-596`

```go
const (
    objectTypeDatabase  // 数据库
    objectTypeTable     // 表
    objectTypeFunction  // 函数
    objectTypeAccount   // 账户
)
```

---

## 7. 权限授予与撤销

**代码位置**: `pkg/frontend/authenticate.go:4964-5063` (doGrantPrivilege)
**代码位置**: `pkg/frontend/authenticate.go:4710-` (doRevokePrivilege)

### 7.1 授予权限 (GRANT)

#### 语法
```sql
-- 授予表级权限
GRANT privilege_list ON [TABLE] table_name TO role_name [WITH GRANT OPTION];

-- 授予数据库级权限
GRANT privilege_list ON DATABASE database_name TO role_name [WITH GRANT OPTION];

-- 授予全局权限
GRANT privilege_list ON *.* TO role_name [WITH GRANT OPTION];

-- 授予角色给用户
GRANT role_name TO user_name [WITH GRANT OPTION];
```

#### 示例
```sql
-- 授予表的SELECT权限
GRANT SELECT ON my_db.my_table TO my_role;

-- 授予数据库所有权限
GRANT ALL PRIVILEGES ON DATABASE my_db TO my_role;

-- 授予全局读权限
GRANT SELECT ON *.* TO readonly_role;

-- 授予角色给用户
GRANT my_role TO my_user;

-- 授予权限并允许再授权
GRANT SELECT ON my_db.my_table TO my_role WITH GRANT OPTION;
```

### 7.2 撤销权限 (REVOKE)

#### 语法
```sql
-- 撤销表级权限
REVOKE privilege_list ON [TABLE] table_name FROM role_name;

-- 撤销数据库级权限
REVOKE privilege_list ON DATABASE database_name FROM role_name;

-- 撤销全局权限
REVOKE privilege_list ON *.* FROM role_name;

-- 撤销角色
REVOKE role_name FROM user_name;
```

#### 示例
```sql
-- 撤销表的SELECT权限
REVOKE SELECT ON my_db.my_table FROM my_role;

-- 撤销数据库所有权限
REVOKE ALL PRIVILEGES ON DATABASE my_db FROM my_role;

-- 撤销角色
REVOKE my_role FROM my_user;
```

### 7.3 WITH GRANT OPTION

`WITH GRANT OPTION` 允许被授权者将权限再授予其他角色。

**代码位置**: `pkg/frontend/authenticate.go:1861-1899` (权限检查SQL生成)

---

## 8. 系统租户权限机制

**代码位置**: `pkg/frontend/authenticate2.go:24-44`

### 8.1 系统租户 (sys)

- 账户ID: `0`
- 账户名: `"sys"`
- 特权: 可以完全操作 `mo_catalog` 数据库的集群表

### 8.2 集群表权限控制

```go
func verifyAccountCanOperateClusterTable(
    account *TenantInfo,
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

### 8.3 集群表操作类型

**代码位置**: `pkg/frontend/authenticate.go:2131-2148`

```go
const (
    clusterTableNone      // 不操作
    clusterTableSelect    // 只读选择
    clusterTableModify    // 修改操作（INSERT/UPDATE/DELETE）
)
```

### 8.4 系统保护数据库

以下数据库受到特殊保护，普通租户用户不能直接写入：

- `mo_catalog` - 系统元数据
- `information_schema` - 信息架构
- `system` - 系统数据库
- `system_metrics` - 系统指标
- `mysql` - MySQL兼容性

**代码位置**: `pkg/frontend/authenticate2.go:51-74`

---

## 9. 权限检查流程

**代码位置**: `pkg/frontend/authenticate.go:6269-6540`

### 9.1 权限检查层次

```
1. 轻量级检查 (verifyLightPrivilege)
   ├─ 检查是否为系统数据库操作
   ├─ 检查是否为集群表操作
   └─ 不需要访问权限表
       ↓
2. 缓存检查 (checkPrivilegeInCache)
   ├─ 查询权限缓存
   ├─ 按账户/数据库/表三级缓存
   └─ 缓存命中则直接返回
       ↓
3. 完整检查 (determineUserHasPrivilegeSet)
   ├─ 访问 mo_role_privs 表
   ├─ 检查用户的所有角色
   ├─ 检查角色继承链
   └─ 更新权限缓存
```

### 9.2 权限检查主函数

**代码位置**: `pkg/frontend/authenticate.go:6429-6540`

```go
func determineUserHasPrivilegeSet(
    ctx context.Context,
    ses *Session,
    cache *privilegeCache,
    entries []privilegeEntry,
    pls []privilegeLevelType) (bool, error)
```

### 9.3 权限缓存结构

**代码位置**: `pkg/frontend/authenticate.go:2352-2410`

```go
type privilegeCache struct {
    // 表权限缓存
    storeForTable [...]btree.Set[PrivilegeType]
    storeForTable2 btree.Map[string, *btree.Set]
    storeForTable3 btree.Map[string, *btree.Map]
    
    // 数据库权限缓存
    storeForDatabase [...]btree.Set[PrivilegeType]
    storeForDatabase2 btree.Map[string, *btree.Set]
    
    // 账户权限缓存
    storeForAccount [...]btree.Set[PrivilegeType]
    
    // 缓存统计
    total atomic.Uint64
    hit atomic.Uint64
}
```

### 9.4 权限检查SQL生成

**代码位置**: `pkg/frontend/authenticate.go:1837-1933`

系统通过以下函数生成SQL查询来检查权限：

- `getSqlForCheckRoleHasPrivilege()` - 基础权限检查
- `getSqlForCheckRoleHasTableLevelPrivilege()` - 表级权限检查
- `getSqlForCheckRoleHasDatabaseLevelForDatabase()` - 数据库级权限检查
- `getSqlForCheckRoleHasAccountLevelForStar()` - 账户级权限检查
- `getSqlForCheckWithGrantOption...()` - WITH GRANT OPTION 检查

---

## 10. 只读权限配置

### 10.1 只读权限定义

要创建只读权限，只需授予 `SELECT` 权限，不授予任何写权限（INSERT, UPDATE, DELETE, TRUNCATE）。

### 10.2 权限范围 (Privilege Scope)

**代码位置**: `pkg/frontend/authenticate.go:687-732`

```go
const (
    PrivilegeScopeSys      = 1   // 系统级
    PrivilegeScopeAccount  = 2   // 账户级
    PrivilegeScopeUser     = 4   // 用户级
    PrivilegeScopeRole     = 8   // 角色级
    PrivilegeScopeDatabase = 16  // 数据库级
    PrivilegeScopeTable    = 32  // 表级
    PrivilegeScopeRoutine  = 64  // 存储过程级
)
```

---

## 11. 常见场景示例

### 11.1 创建只读角色（全局）

```sql
-- 创建只读角色
CREATE ROLE readonly_role;

-- 授予全局SELECT权限
GRANT SELECT ON *.* TO readonly_role;

-- 授予CONNECT权限（允许连接）
GRANT CONNECT ON ACCOUNT * TO readonly_role;

-- 授予SHOW DATABASES权限（允许列出数据库）
GRANT SHOW DATABASES ON ACCOUNT * TO readonly_role;

-- 授予SHOW TABLES权限（允许列出表）
GRANT SHOW TABLES ON *.* TO readonly_role;

-- 创建用户并授予角色
CREATE USER readonly_user IDENTIFIED BY 'password' DEFAULT ROLE readonly_role;
GRANT readonly_role TO readonly_user;
```

### 11.2 创建只读角色（特定数据库）

```sql
-- 创建只读角色
CREATE ROLE db_readonly_role;

-- 授予特定数据库的SELECT权限
GRANT SELECT ON my_database.* TO db_readonly_role;

-- 授予CONNECT权限
GRANT CONNECT ON ACCOUNT * TO db_readonly_role;

-- 授予SHOW DATABASES和SHOW TABLES权限
GRANT SHOW DATABASES ON ACCOUNT * TO db_readonly_role;
GRANT SHOW TABLES ON DATABASE my_database TO db_readonly_role;

-- 创建用户并授予角色
CREATE USER db_readonly_user IDENTIFIED BY 'password' DEFAULT ROLE db_readonly_role;
GRANT db_readonly_role TO db_readonly_user;
```

### 11.3 创建只读角色（特定表）

```sql
-- 创建只读角色
CREATE ROLE table_readonly_role;

-- 授予特定表的SELECT权限
GRANT SELECT ON my_database.table1 TO table_readonly_role;
GRANT SELECT ON my_database.table2 TO table_readonly_role;
GRANT SELECT ON my_database.table3 TO table_readonly_role;

-- 授予CONNECT权限
GRANT CONNECT ON ACCOUNT * TO table_readonly_role;

-- 授予必要的浏览权限
GRANT SHOW DATABASES ON ACCOUNT * TO table_readonly_role;
GRANT SHOW TABLES ON DATABASE my_database TO table_readonly_role;

-- 创建用户并授予角色
CREATE USER table_readonly_user IDENTIFIED BY 'password' DEFAULT ROLE table_readonly_role;
GRANT table_readonly_role TO table_readonly_user;
```

### 11.4 sys租户创建只读账号

**注意**: sys租户具有特殊权限，可以操作所有集群表。

```sql
-- 在sys租户中创建只读角色
CREATE ROLE sys_readonly_role;

-- 授予必要的连接和浏览权限
GRANT CONNECT ON ACCOUNT * TO sys_readonly_role;
GRANT SHOW DATABASES ON ACCOUNT * TO sys_readonly_role;
GRANT SHOW TABLES ON *.* TO sys_readonly_role;

-- 授予特定库表的SELECT权限
GRANT SELECT ON mo_catalog.mo_tables TO sys_readonly_role;
GRANT SELECT ON mo_catalog.mo_database TO sys_readonly_role;
GRANT SELECT ON mo_catalog.mo_columns TO sys_readonly_role;

-- 如果需要查看其他系统表
GRANT SELECT ON system.statement_info TO sys_readonly_role;

-- 创建只读用户
CREATE USER sys_readonly_user IDENTIFIED BY 'readonly_password' DEFAULT ROLE sys_readonly_role;
GRANT sys_readonly_role TO sys_readonly_user;
```

### 11.5 分级权限管理

```sql
-- 开发环境：读写权限
CREATE ROLE dev_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_db.* TO dev_role;

-- 测试环境：只读权限
CREATE ROLE test_role;
GRANT SELECT ON test_db.* TO test_role;

-- 生产环境：只读权限
CREATE ROLE prod_readonly_role;
GRANT SELECT ON prod_db.* TO prod_readonly_role;

-- 生产环境：管理员权限
CREATE ROLE prod_admin_role;
GRANT ALL PRIVILEGES ON prod_db.* TO prod_admin_role;
```

### 11.6 检查用户权限

```sql
-- 查看用户的角色
SELECT r.role_name, ug.with_grant_option
FROM mo_catalog.mo_user_grant ug
JOIN mo_catalog.mo_role r ON ug.role_id = r.role_id
JOIN mo_catalog.mo_user u ON ug.user_id = u.user_id
WHERE u.user_name = 'username';

-- 查看角色的权限
SELECT 
    role_name,
    obj_type,
    privilege_name,
    privilege_level,
    with_grant_option
FROM mo_catalog.mo_role_privs
WHERE role_name = 'role_name'
ORDER BY obj_type, privilege_level;

-- 查看特定数据库的权限
SELECT 
    r.role_name,
    rp.privilege_name,
    rp.privilege_level,
    rp.with_grant_option
FROM mo_catalog.mo_role_privs rp
JOIN mo_catalog.mo_role r ON rp.role_id = r.role_id
WHERE rp.privilege_level LIKE 'database_name%'
   OR rp.privilege_level IN ('*.*', '*');
```

### 11.7 撤销写权限（降级为只读）

```sql
-- 撤销写权限
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON database_name.* FROM role_name;

-- 保留读权限（如果未撤销则保持）
-- SELECT 权限保持不变
```

### 11.8 临时授予写权限

```sql
-- 创建临时写权限角色
CREATE ROLE temp_write_role;
GRANT SELECT, INSERT, UPDATE ON my_db.my_table TO temp_write_role;

-- 临时授予用户
GRANT temp_write_role TO user_name;

-- 工作完成后撤销
REVOKE temp_write_role FROM user_name;

-- 可选：删除临时角色
DROP ROLE temp_write_role;
```

---

## 12. 权限最佳实践

### 12.1 最小权限原则

- 只授予完成工作所需的最小权限集
- 优先使用角色而非直接授予用户权限
- 避免授予 `ALL PRIVILEGES` 除非绝对必要

### 12.2 角色管理

- 为不同的职能创建不同的角色（开发、测试、只读、管理员等）
- 使用角色继承来构建权限层次
- 定期审计角色权限

### 12.3 只读访问

- 对于分析和报表用户，只授予 SELECT 权限
- 对于生产环境，大多数用户应该只有只读权限
- 监控和审计系统应该使用只读账号

### 12.4 权限审计

```sql
-- 定期审查具有高权限的用户
SELECT 
    u.user_name,
    r.role_name,
    rp.privilege_name,
    rp.privilege_level
FROM mo_catalog.mo_user u
JOIN mo_catalog.mo_user_grant ug ON u.user_id = ug.user_id
JOIN mo_catalog.mo_role r ON ug.role_id = r.role_id
JOIN mo_catalog.mo_role_privs rp ON r.role_id = rp.role_id
WHERE rp.privilege_name IN ('ACCOUNT_ALL', 'DATABASE_ALL', 'TABLE_ALL')
ORDER BY u.user_name, r.role_name;
```

### 12.5 系统租户保护

- 限制sys租户访问，仅用于系统管理
- 普通应用不应使用sys租户连接
- 为sys租户设置强密码策略

---

## 13. 故障排查

### 13.1 权限拒绝错误

```sql
-- 检查用户是否存在
SELECT * FROM mo_catalog.mo_user WHERE user_name = 'username';

-- 检查用户的角色分配
SELECT r.* 
FROM mo_catalog.mo_user_grant ug
JOIN mo_catalog.mo_role r ON ug.role_id = r.role_id
WHERE ug.user_id = (SELECT user_id FROM mo_catalog.mo_user WHERE user_name = 'username');

-- 检查角色的权限
SELECT * 
FROM mo_catalog.mo_role_privs 
WHERE role_id IN (
    SELECT role_id 
    FROM mo_catalog.mo_user_grant 
    WHERE user_id = (SELECT user_id FROM mo_catalog.mo_user WHERE user_name = 'username')
);
```

### 13.2 权限缓存问题

权限更改后，缓存会自动失效。如果遇到权限不生效的情况：

1. 重新连接数据库（建立新会话）
2. 检查权限是否正确授予
3. 确认用户是否激活了正确的角色

### 13.3 WITH GRANT OPTION 问题

```sql
-- 检查是否有WITH GRANT OPTION
SELECT 
    role_name,
    privilege_name,
    with_grant_option
FROM mo_catalog.mo_role_privs
WHERE role_id = (SELECT role_id FROM mo_catalog.mo_role WHERE role_name = 'role_name')
  AND privilege_name = 'SELECT';
```

---

## 14. 相关代码文件索引

| 功能 | 文件路径 | 关键函数/结构 |
|------|---------|--------------|
| 权限核心逻辑 | `pkg/frontend/authenticate.go` | `determineUserHasPrivilegeSet`, `doGrantPrivilege`, `doRevokePrivilege` |
| 轻量级权限检查 | `pkg/frontend/authenticate2.go` | `verifyLightPrivilege`, `verifyAccountCanOperateClusterTable` |
| 系统表定义 | `pkg/frontend/predefined.go` | `MoCatalogMoUserDDL`, `MoCatalogMoRolePrivsDDL` |
| GRANT语法解析 | `pkg/sql/parsers/tree/grant.go` | `GrantPrivilege`, `GrantRole` |
| REVOKE语法解析 | `pkg/sql/parsers/tree/revoke.go` | `Privilege`, `PrivilegeLevel`, `PrivilegeType` |
| 权限检查入口 | `pkg/frontend/query_result.go` | `checkPrivilege` |
| 计算包装器 | `pkg/frontend/computation_wrapper.go` | `checkResultQueryPrivilege` |

---

## 15. 总结

MatrixOne 的权限管理系统提供了：

1. **多层次权限控制**: 从系统租户到表级的7级权限层次
2. **细粒度权限**: 34种权限类型覆盖所有数据库操作
3. **基于角色的访问控制**: 使用RBAC模型简化权限管理
4. **高效的权限缓存**: 使用btree数据结构实现多维度缓存
5. **系统租户隔离**: sys租户对集群表的完全控制
6. **灵活的只读权限**: 支持全局、数据库级、表级只读配置

对于创建只读账号的需求，MatrixOne完全支持在任何租户（包括sys租户）中创建只读角色和用户，并可以精确控制到特定库表的读权限。
