# create_readonly_account.sh 脚本详细分析

## 概述

这是一个 **Bash 自动化脚本**，用于在 MatrixOne 的 sys 租户中创建只读账号。它将手动的 10+ 步操作简化为一条命令。

## 脚本做了什么？

### 核心功能

**一句话总结**：连接 MatrixOne 数据库 → 创建只读角色 → 授予指定数据库的 SELECT 权限 → 创建用户并关联角色 → 自动验证

---

## 执行流程详解

### 阶段 1: 初始化和参数解析 (行 1-143)

#### 1.1 设置默认值 (行 14-22)

```bash
HOST="127.0.0.1"          # 数据库主机
PORT="6001"               # 数据库端口
SYS_USER="root"           # 管理员用户名
SYS_PASSWORD=""           # 管理员密码（必填）
READONLY_USER="moi_readonly"      # 要创建的只读用户名
READONLY_PASSWORD=""      # 只读用户密码（必填）
ROLE_NAME="moi_readonly_role"     # 要创建的角色名
DATABASE="moi"            # 要授权的数据库名
```

**作用**: 提供合理的默认值，用户只需提供必要参数

#### 1.2 定义辅助函数 (行 24-41)

```bash
print_info()   # 绿色信息输出
print_warn()   # 黄色警告输出
print_error()  # 红色错误输出
```

**作用**: 提供彩色输出，提升用户体验

#### 1.3 解析命令行参数 (行 65-110)

支持的参数：
- `-h <host>` - 数据库主机地址
- `-P <port>` - 数据库端口
- `-u <user>` - sys 管理员用户名
- `-p <password>` - sys 管理员密码 ⚠️ **必需**
- `-n <name>` - 只读用户名
- `-w <password>` - 只读用户密码 ⚠️ **必需**
- `-d <database>` - 要授权的数据库名
- `-r <role>` - 角色名
- `--help` - 显示帮助

**示例**:
```bash
./create_readonly_account.sh \
  -h 127.0.0.1 \
  -P 6001 \
  -u root \
  -p "admin_password" \
  -n moi_readonly \
  -w "readonly_password"
```

#### 1.4 参数验证 (行 112-129)

```bash
# 检查必需参数
if [ -z "$SYS_PASSWORD" ]; then
    print_error "缺少 sys 租户管理员密码 (-p)"
    exit 1
fi

if [ -z "$READONLY_PASSWORD" ]; then
    print_error "缺少只读用户密码 (-w)"
    exit 1
fi

# 检查 mysql 客户端是否安装
if ! command -v mysql &> /dev/null; then
    print_error "未找到 mysql 客户端，请先安装 mysql-client"
    exit 1
fi
```

**作用**: 
- 确保必需参数已提供
- 确保运行环境具备 mysql 客户端

---

### 阶段 2: 预检查 (行 144-177)

#### 2.1 测试数据库连接 (行 144-150)

```bash
echo "SELECT 1;" | $MYSQL_CMD &> /dev/null
```

**作用**: 在执行任何操作前验证数据库连接是否正常

**如果失败**: 立即退出，提示用户检查连接参数

#### 2.2 检查目标数据库是否存在 (行 152-159)

```sql
SELECT COUNT(*) FROM mo_catalog.mo_database WHERE datname='${DATABASE}';
```

**作用**: 确保要授权的数据库存在

**如果不存在**: 退出并提示用户先创建数据库

#### 2.3 检查角色是否已存在 (行 161-169)

```sql
SELECT COUNT(*) FROM mo_catalog.mo_role WHERE role_name='${ROLE_NAME}';
```

**作用**: 
- 如果角色已存在 → 跳过角色创建，只创建用户
- 如果角色不存在 → 创建新角色并授权

**智能处理**: 允许多个用户共享同一个只读角色

#### 2.4 检查用户是否已存在 (行 171-177)

```sql
SELECT COUNT(*) FROM mo_catalog.mo_user WHERE user_name='${READONLY_USER}';
```

**作用**: 防止用户名冲突

**如果存在**: 退出并提示用户更换用户名或删除现有用户

---

### 阶段 3: 生成 SQL 脚本 (行 179-246)

这是脚本的**核心部分**，动态生成 SQL 语句。

#### 3.1 条件逻辑：创建角色（如果不存在）

```sql
-- 如果角色不存在，执行以下步骤：

-- 步骤 1: 创建角色
CREATE ROLE moi_readonly_role;

-- 步骤 2: 授予基础权限
GRANT CONNECT ON ACCOUNT * TO moi_readonly_role;
GRANT SHOW DATABASES ON ACCOUNT * TO moi_readonly_role;
GRANT SHOW TABLES ON DATABASE moi TO moi_readonly_role;

-- 步骤 3: 授予数据库 SELECT 权限
GRANT SELECT ON TABLE moi.* TO moi_readonly_role;

-- 步骤 4: 授予系统表权限（用于查看表结构）
GRANT SELECT ON TABLE mo_catalog.mo_tables TO moi_readonly_role;
GRANT SELECT ON TABLE mo_catalog.mo_columns TO moi_readonly_role;
```

**权限说明**:

| 权限 | 作用 | 必要性 |
|------|------|--------|
| `CONNECT ON ACCOUNT *` | 允许连接到数据库 | ✅ 必需 |
| `SHOW DATABASES ON ACCOUNT *` | 允许查看数据库列表 | 🔹 可选（方便操作） |
| `SHOW TABLES ON DATABASE moi` | 允许查看 moi 数据库的表列表 | 🔹 可选（方便操作） |
| `SELECT ON TABLE moi.*` | 允许读取 moi 数据库所有表 | ✅ 必需 |
| `SELECT ON TABLE mo_catalog.mo_tables` | 允许查看表元数据 | 🔹 可选（查看表结构） |
| `SELECT ON TABLE mo_catalog.mo_columns` | 允许查看列元数据 | 🔹 可选（查看列信息） |

#### 3.2 创建用户

```sql
-- 步骤 5: 创建只读用户
CREATE USER moi_readonly
    IDENTIFIED BY 'readonly_password'
    DEFAULT ROLE moi_readonly_role;

-- 步骤 6: 将角色授予用户
GRANT moi_readonly_role TO moi_readonly;
```

**说明**:
- `IDENTIFIED BY` - 设置用户密码
- `DEFAULT ROLE` - 设置默认角色，用户登录时自动激活
- `GRANT role TO user` - 将角色授予用户

#### 3.3 验证配置

```sql
-- 查询用户信息
SELECT user_name, status, created_time, default_role
FROM mo_catalog.mo_user
WHERE user_name = 'moi_readonly';

-- 查询用户角色关联
SELECT u.user_name, r.role_name, ug.granted_time
FROM mo_catalog.mo_user u
JOIN mo_catalog.mo_user_grant ug ON u.user_id = ug.user_id
JOIN mo_catalog.mo_role r ON ug.role_id = r.role_id
WHERE u.user_name = 'moi_readonly';

-- 查询角色权限
SELECT role_name, obj_type, privilege_name, privilege_level
FROM mo_catalog.mo_role_privs
WHERE role_name = 'moi_readonly_role'
ORDER BY obj_type, privilege_level, privilege_name;
```

**作用**: 显示创建结果，方便用户确认

---

### 阶段 4: 执行 SQL 脚本 (行 248-270)

```bash
if echo "$SQL_SCRIPT" | $MYSQL_CMD; then
    # 成功 - 显示成功信息和测试命令
else
    # 失败 - 显示错误并退出
fi
```

**执行流程**:
1. 通过管道将 SQL 脚本传递给 mysql 客户端
2. 如果成功，显示详细的成功信息
3. 如果失败，显示错误信息并退出

**输出示例**:
```
[INFO] 只读账号创建成功！
[INFO] 数据库: moi
[INFO] 角色: moi_readonly_role
[INFO] 用户名: moi_readonly
[INFO] 密码: readonly_password
[INFO] 测试连接命令:
  mysql -h127.0.0.1 -P6001 -umoi_readonly -preadonly_password
```

---

### 阶段 5: 自动验证测试 (行 272-287)

#### 5.1 连接测试

```bash
echo "SELECT 1 AS test;" | mysql -h${HOST} -P${PORT} -u${READONLY_USER} -p${READONLY_PASSWORD}
```

**作用**: 验证新创建的只读用户能否成功连接

**如果失败**: 说明用户创建或权限配置有问题

#### 5.2 读权限测试

```bash
echo "SELECT COUNT(*) FROM mo_catalog.mo_tables;" | mysql ... -u${READONLY_USER}
```

**作用**: 验证只读用户是否有基本的读权限

**如果失败**: 显示警告但不退出（可能是数据库为空）

---

## 脚本的安全特性

### ✅ 安全措施

1. **参数验证**
   - 检查必需参数
   - 验证数据库连接
   - 检查目标数据库存在性

2. **冲突检测**
   - 检查用户名是否已存在
   - 智能处理角色已存在的情况

3. **原子性**
   - 使用 `set -e`，任何命令失败立即退出
   - 防止部分创建导致不一致状态

4. **最小权限原则**
   - 只授予 SELECT 权限
   - 不授予任何写操作权限（INSERT/UPDATE/DELETE/CREATE/DROP）

5. **自动验证**
   - 创建后立即测试连接
   - 验证权限配置正确

### ⚠️ 安全注意事项

1. **命令行密码**
   - 密码通过命令行参数传递，可能在进程列表中可见
   - 建议：使用环境变量或密码文件

2. **密码强度**
   - 脚本不检查密码强度
   - 建议：使用强密码（混合大小写、数字、特殊字符）

3. **日志记录**
   - mysql 客户端可能在历史文件中记录密码
   - 建议：执行后清理 `.mysql_history`

---

## 实际执行示例

### 示例 1: 基本用法

```bash
./optools/create_readonly_account.sh \
  -h 127.0.0.1 \
  -P 6001 \
  -u dump \
  -p "111" \
  -n moi_readonly \
  -w "123"
```

**执行步骤**:
1. ✓ 连接到 127.0.0.1:6001
2. ✓ 检查 moi 数据库存在
3. ✓ 创建角色 `moi_readonly_role`
4. ✓ 授予权限（CONNECT, SHOW DATABASES, SHOW TABLES, SELECT）
5. ✓ 创建用户 `moi_readonly`
6. ✓ 关联角色到用户
7. ✓ 显示配置信息
8. ✓ 测试连接
9. ✓ 完成

### 示例 2: 为不同数据库创建只读账号

```bash
./optools/create_readonly_account.sh \
  -h 10.0.0.100 \
  -P 6001 \
  -u root \
  -p "AdminPass@2024" \
  -d analytics_db \
  -n analytics_reader \
  -w "ReaderPass@2024" \
  -r analytics_readonly_role
```

**效果**:
- 数据库: `analytics_db`
- 角色: `analytics_readonly_role`
- 用户: `analytics_reader`
- 权限: analytics_db 所有表的 SELECT 权限

---

## 技术细节

### 1. 使用 HEREDOC 生成 SQL

```bash
SQL_SCRIPT=$(cat <<EOF
-- SQL 语句
CREATE ROLE ...;
GRANT ...;
EOF
)
```

**优势**:
- SQL 代码可读性好
- 支持变量插值（`${DATABASE}`）
- 支持条件逻辑（if-then-else）

### 2. 条件 SQL 生成

```bash
$(if [ "$SKIP_ROLE_CREATE" -eq "0" ]; then
    echo "CREATE ROLE ${ROLE_NAME};"
    echo "GRANT ...;"
else
    echo "-- 角色已存在，跳过"
fi)
```

**优势**:
- 根据预检查结果动态生成 SQL
- 避免重复创建角色导致错误

### 3. 静默输出控制

```bash
$MYSQL_CMD &> /dev/null    # 完全静默
$MYSQL_CMD -sN             # 只输出结果，无表格边框
```

**用途**:
- 预检查时静默执行
- 获取结果时使用 `-sN` 去除格式

### 4. 错误处理

```bash
set -e                     # 任何命令失败立即退出

if ! command; then
    print_error "..."
    exit 1
fi
```

**保证**:
- 失败时不会继续执行
- 提供清晰的错误信息

---

## 与手动操作的对比

### 手动操作（10+ 步骤）

```sql
-- 1. 连接数据库
mysql -h... -u... -p...

-- 2. 创建角色
CREATE ROLE moi_readonly_role;

-- 3. 授予基础权限
GRANT CONNECT ON ACCOUNT * TO moi_readonly_role;
GRANT SHOW DATABASES ON ACCOUNT * TO moi_readonly_role;
GRANT SHOW TABLES ON DATABASE moi TO moi_readonly_role;

-- 4. 授予 SELECT 权限
GRANT SELECT ON TABLE moi.* TO moi_readonly_role;
GRANT SELECT ON TABLE mo_catalog.mo_tables TO moi_readonly_role;
GRANT SELECT ON TABLE mo_catalog.mo_columns TO moi_readonly_role;

-- 5. 创建用户
CREATE USER moi_readonly IDENTIFIED BY '...' DEFAULT ROLE moi_readonly_role;

-- 6. 授予角色
GRANT moi_readonly_role TO moi_readonly;

-- 7. 验证用户
SELECT * FROM mo_catalog.mo_user WHERE user_name = 'moi_readonly';

-- 8. 验证角色
SELECT * FROM mo_catalog.mo_user_grant WHERE ...;

-- 9. 验证权限
SELECT * FROM mo_catalog.mo_role_privs WHERE ...;

-- 10. 测试连接
mysql -h... -umoi_readonly -p...

-- 11. 测试权限
SELECT * FROM moi.some_table;
```

**问题**:
- ❌ 步骤多，容易遗漏
- ❌ 容易出错（语法、权限级别）
- ❌ 重复劳动
- ❌ 没有自动验证

### 使用脚本（1 条命令）

```bash
./optools/create_readonly_account.sh -h ... -p ... -w ...
```

**优势**:
- ✅ 一条命令完成所有操作
- ✅ 自动验证每个步骤
- ✅ 智能处理冲突
- ✅ 详细的执行日志
- ✅ 自动测试连接和权限
- ✅ 可复用、可自动化

---

## 适用场景

### ✅ 适合使用脚本的场景

1. **批量创建只读账号**
   ```bash
   for db in db1 db2 db3; do
       ./create_readonly_account.sh -d $db -n ${db}_reader -w "password"
   done
   ```

2. **CI/CD 自动化部署**
   ```yaml
   - name: Create readonly account
     run: |
       ./optools/create_readonly_account.sh \
         -h $DB_HOST \
         -p $ADMIN_PASSWORD \
         -w $READONLY_PASSWORD
   ```

3. **标准化账号创建流程**
   - 确保所有只读账号权限一致
   - 减少人为错误

4. **快速测试环境搭建**
   - 开发/测试环境快速创建只读账号

### ⚠️ 不适合的场景

1. **需要更复杂的权限配置**
   - 例如：只授予特定几张表的权限
   - 解决方案：手动执行或修改脚本

2. **需要授予额外的系统表权限**
   - 例如：mo_catalog.mo_account
   - 解决方案：脚本执行后手动补充

---

## 总结

### 脚本的核心价值

1. **自动化** - 将 10+ 步手动操作简化为 1 条命令
2. **安全性** - 遵循最小权限原则，只授予必要的只读权限
3. **可靠性** - 完整的错误检查和自动验证
4. **可维护性** - 代码结构清晰，易于理解和修改
5. **用户友好** - 彩色输出、详细日志、自动测试

### 脚本做了什么（总结）

```
输入: 数据库连接信息 + 要创建的用户名/密码
     ↓
[1] 参数验证和预检查
     ↓
[2] 创建只读角色（如果不存在）
     ↓
[3] 授予权限:
    - CONNECT (连接数据库)
    - SHOW DATABASES (查看数据库列表)
    - SHOW TABLES (查看表列表)
    - SELECT on database.* (读取数据)
    - SELECT on mo_catalog.mo_tables/mo_columns (查看元数据)
     ↓
[4] 创建只读用户并关联角色
     ↓
[5] 自动验证和测试
     ↓
输出: 可用的只读账号 + 测试命令
```

### 关键技术点

- ✅ Bash 脚本 + MySQL 客户端
- ✅ 动态 SQL 生成（HEREDOC + 变量插值）
- ✅ 条件逻辑（角色复用）
- ✅ 完整的错误处理（set -e + 验证）
- ✅ 用户体验（彩色输出 + 详细日志）

---

**这个脚本的最大价值：让创建只读账号从"复杂的多步操作"变成"一条简单的命令"！**
