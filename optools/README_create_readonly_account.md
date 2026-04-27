# 创建只读账号脚本使用说明

## 功能

`create_readonly_account.sh` 是一个用于在 MatrixOne sys 租户中快速创建只读账号的自动化脚本。

## 特性

### 核心功能
- ✅ 自动创建只读角色和用户
- ✅ 授予指定数据库所有表的 SELECT 权限
- ✅ 授予必要的 mo_catalog 表权限（mo_tables, mo_columns）
- ✅ 自动验证配置和连接测试
- ✅ 详细的执行日志和错误处理
- ✅ 支持角色复用（如果角色已存在则跳过创建）

### 🆕 v2.0 新功能
- ✅ **支持多数据库授权** - 创建账号时一次性授权多个数据库
- ✅ **增量添加权限** - 为已存在用户添加新数据库权限（使用 `--add-db`）
- ✅ **批量操作** - 支持逗号分隔的数据库列表

## 使用方法

### 基本用法

```bash
./create_readonly_account.sh \
  -h <host> \
  -P <port> \
  -u <sys-user> \
  -p <sys-password> \
  -n <readonly-username> \
  -w <readonly-password>
```

### 参数说明

| 参数 | 说明 | 默认值 | 必需 |
|------|------|--------|------|
| `-h <host>` | 数据库主机地址 | 127.0.0.1 | 否 |
| `-P <port>` | 数据库端口 | 6001 | 否 |
| `-u <user>` | sys 租户管理员用户名 | root | 否 |
| `-p <password>` | sys 租户管理员密码 | - | **是** |
| `-n <name>` | 只读用户名 | moi_readonly | 否 |
| `-w <password>` | 只读用户的密码 | - | 创建新用户时**必需** |
| `-d <database>` | 要授权的数据库名（支持逗号分隔多个） | moi | 否 |
| `-r <role>` | 要创建的角色名 | moi_readonly_role | 否 |
| `--add-db <db>` | 为已存在用户添加数据库权限（支持逗号分隔） | - | 添加权限时**必需** |
| `--help` | 显示帮助信息 | - | - |

### 示例

#### 示例 1: 创建默认的 moi 数据库只读账号

```bash
./create_readonly_account.sh \
  -h 127.0.0.1 \
  -P 6001 \
  -u dump \
  -p "111" \
  -n moi_readonly \
  -w "123"
```

#### 示例 2: 为其他数据库创建只读账号

```bash
./create_readonly_account.sh \
  -h 10.0.0.100 \
  -P 6001 \
  -u root \
  -p "AdminPass@2024" \
  -d analytics_db \
  -n analytics_reader \
  -w "ReaderPass@2024" \
  -r analytics_readonly_role
```

#### 示例 3: 🆕 创建账号并授权多个数据库

```bash
./create_readonly_account.sh \
  -h 127.0.0.1 \
  -P 6001 \
  -u dump \
  -p "111" \
  -n multi_db_reader \
  -w "pass123" \
  -d "moi,moi_main,analytics_db"
```

**效果**: 创建用户 `multi_db_reader`，同时授予 `moi`、`moi_main`、`analytics_db` 三个数据库的读权限。

#### 示例 4: 🆕 为已存在用户添加新数据库权限

```bash
# 添加单个数据库
./create_readonly_account.sh \
  -h 127.0.0.1 \
  -P 6001 \
  -u dump \
  -p "111" \
  -n moi_readonly \
  --add-db moi_main

# 添加多个数据库
./create_readonly_account.sh \
  -u dump \
  -p "111" \
  -n moi_readonly \
  --add-db "test_db1,test_db2,test_db3"
```

**特点**:
- ✅ 不需要提供 `-w` (用户密码) 参数
- ✅ 自动检查用户和角色是否存在
- ✅ 支持一次性添加多个数据库

#### 示例 5: 使用环境变量传递密码（更安全）

```bash
# 设置环境变量
export SYS_PASSWORD="AdminPass@2024"
export READONLY_PASSWORD="ReaderPass@2024"

# 运行脚本
./create_readonly_account.sh \
  -h 127.0.0.1 \
  -P 6001 \
  -u root \
  -p "$SYS_PASSWORD" \
  -n moi_readonly \
  -w "$READONLY_PASSWORD"
```

## 脚本执行流程

1. **参数验证** - 检查必需参数是否提供
2. **连接测试** - 验证数据库连接是否正常
3. **数据库检查** - 确认目标数据库存在
4. **角色检查** - 检查角色是否已存在
5. **用户检查** - 确认用户名未被占用
6. **创建角色** - 创建只读角色（如果不存在）
7. **授予权限** - 授予必要的连接、查看和 SELECT 权限
8. **创建用户** - 创建只读用户并关联角色
9. **验证配置** - 显示用户信息、角色和权限
10. **连接测试** - 测试只读用户是否能正常连接

## 授予的权限

脚本会授予以下权限：

### 1. 基础权限
- `CONNECT ON ACCOUNT *` - 连接到数据库
- `SHOW DATABASES ON ACCOUNT *` - 查看数据库列表
- `SHOW TABLES ON DATABASE <db>` - 查看表列表

### 2. 数据权限
- `SELECT ON TABLE <database>.*` - 读取指定数据库所有表
- `SELECT ON TABLE mo_catalog.mo_tables` - 查看表元数据
- `SELECT ON TABLE mo_catalog.mo_columns` - 查看列元数据

## 测试只读账号

### 1. 连接测试

```bash
mysql -h127.0.0.1 -P6001 -umoi_readonly -p123
```

### 2. 查看数据库

```bash
echo 'SHOW DATABASES;' | mysql -h127.0.0.1 -P6001 -umoi_readonly -p123
```

### 3. 查看表（需要先 USE 数据库）

```bash
echo 'USE moi; SHOW TABLES;' | mysql -h127.0.0.1 -P6001 -umoi_readonly -p123
```

### 4. 查询数据

```bash
echo 'SELECT * FROM moi.test_table LIMIT 10;' | mysql -h127.0.0.1 -P6001 -umoi_readonly -p123
```

### 5. 验证写操作被拒绝

```bash
# 应该失败 - INSERT
echo "INSERT INTO moi.test_table (col1) VALUES ('test');" | mysql -h127.0.0.1 -P6001 -umoi_readonly -p123

# 应该失败 - UPDATE
echo "UPDATE moi.test_table SET col1='test' WHERE id=1;" | mysql -h127.0.0.1 -P6001 -umoi_readonly -p123

# 应该失败 - DELETE
echo "DELETE FROM moi.test_table WHERE id=1;" | mysql -h127.0.0.1 -P6001 -umoi_readonly -p123

# 应该失败 - CREATE TABLE
echo "CREATE TABLE moi.new_table (id INT);" | mysql -h127.0.0.1 -P6001 -umoi_readonly -p123
```

## 常见问题

### Q1: 脚本执行失败 "数据库连接失败"

**原因**: 主机、端口、用户名或密码不正确

**解决方案**:
```bash
# 先手动测试连接
mysql -h<host> -P<port> -u<user> -p<password>

# 确认连接成功后再运行脚本
```

### Q2: "数据库不存在"

**原因**: 目标数据库尚未创建

**解决方案**:
```bash
# 先创建数据库
echo "CREATE DATABASE moi;" | mysql -h127.0.0.1 -P6001 -udump -p111

# 再运行脚本
```

### Q3: "用户已存在"

**原因**: 用户名已被占用

**解决方案**:
```bash
# 方案 1: 使用不同的用户名
./create_readonly_account.sh -n moi_readonly2 -w "password"

# 方案 2: 删除现有用户（谨慎操作）
echo "DROP USER moi_readonly;" | mysql -h127.0.0.1 -P6001 -udump -p111
```

### Q4: "角色已存在"

**说明**: 这是正常的，脚本会跳过角色创建并复用现有角色

**注意**: 如果角色已存在但权限不完整，需要手动补充权限：

```sql
-- 补充缺失的权限
GRANT SELECT ON TABLE moi.* TO moi_readonly_role;
GRANT SELECT ON TABLE mo_catalog.mo_tables TO moi_readonly_role;
GRANT SELECT ON TABLE mo_catalog.mo_columns TO moi_readonly_role;
```

### Q5: SHOW TABLES 报权限错误

**原因**: MatrixOne 的权限系统要求先 `USE database` 再执行 `SHOW TABLES`

**解决方案**:
```bash
# ❌ 不工作
echo "SHOW TABLES FROM moi;" | mysql -h127.0.0.1 -P6001 -umoi_readonly -p123

# ✅ 正确方式
echo "USE moi; SHOW TABLES;" | mysql -h127.0.0.1 -P6001 -umoi_readonly -p123
```

### Q6: mysql 客户端未找到

**原因**: 系统未安装 mysql 客户端

**解决方案**:

```bash
# macOS
brew install mysql-client

# Ubuntu/Debian
sudo apt-get install mysql-client

# CentOS/RHEL
sudo yum install mysql
```

## 安全建议

### 1. 使用强密码

```bash
# ❌ 弱密码
-w "123"

# ✅ 强密码
-w "Str0ng!P@ssw0rd#2024"
```

### 2. 避免在命令行直接传递密码

```bash
# ✅ 使用环境变量
export READONLY_PASSWORD="YourStrongPassword"
./create_readonly_account.sh -w "$READONLY_PASSWORD"

# 或者使用密码文件
echo "YourStrongPassword" > .readonly_pass
chmod 600 .readonly_pass
./create_readonly_account.sh -w "$(cat .readonly_pass)"
rm .readonly_pass
```

### 3. 定期轮换密码

```sql
-- 定期更换只读用户密码
ALTER USER moi_readonly IDENTIFIED BY 'NewStrongPassword@2024';
```

### 4. 审计只读账号活动

```sql
-- 查询只读用户的访问记录
SELECT 
    user,
    request_at,
    statement,
    exec_plan
FROM system.statement_info
WHERE user = 'moi_readonly'
  AND request_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)
ORDER BY request_at DESC
LIMIT 100;
```

## 清理

如果需要删除创建的只读账号：

```bash
# 方法 1: 使用 mysql 命令
mysql -h127.0.0.1 -P6001 -udump -p111 <<'EOF'
DROP USER moi_readonly;
DROP ROLE moi_readonly_role;
EOF

# 方法 2: 使用 echo
echo "DROP USER moi_readonly; DROP ROLE moi_readonly_role;" | \
  mysql -h127.0.0.1 -P6001 -udump -p111
```

## 相关文档

- [sys 租户只读账号配置指南](../docs/handbooks/20260427-sys-tenant-readonly-account.md)
- [MatrixOne 权限管理文档](https://docs.matrixorigin.cn/zh/docs/latest/MatrixOne/Security/role-priviledge-management/)

## 版本历史

- **v1.0** (2026-04-27)
  - 初始版本
  - 支持创建 moi 数据库只读账号
  - 自动验证和测试功能

## 许可证

与 MatrixOne 项目相同的许可证
