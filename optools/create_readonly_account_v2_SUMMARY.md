# create_readonly_account.sh v2.0 功能总结

## 🎯 核心改进

脚本从"只能创建账号"升级为"创建+管理账号"，支持对已存在账号进行权限增量更新。

---

## 🆕 新增功能

### 1. `--add-db` 参数：为已存在用户添加新数据库权限

**问题**: v1.0 只能在创建用户时授权，后续要添加新数据库需要手动执行 SQL

**解决方案**: 使用 `--add-db` 参数直接为已存在用户添加权限

**用法**:
```bash
# 添加单个数据库
./create_readonly_account.sh -u admin -p pass -n existing_user --add-db new_db

# 添加多个数据库
./create_readonly_account.sh -u admin -p pass -n existing_user --add-db "db1,db2,db3"
```

**特点**:
- ✅ 不需要用户密码 (无需 `-w` 参数)
- ✅ 自动验证用户、角色、数据库是否存在
- ✅ 支持批量添加（逗号分隔）
- ✅ 自动授予 SHOW TABLES 和 SELECT 权限

### 2. 多数据库授权：创建用户时一次性授权多个数据库

**问题**: v1.0 只能授权单个数据库，多数据库需要多次操作

**解决方案**: `-d` 参数支持逗号分隔的数据库列表

**用法**:
```bash
./create_readonly_account.sh -u admin -p pass \
  -n new_user -w password \
  -d "db1,db2,db3"
```

**特点**:
- ✅ 一次性授权多个数据库
- ✅ 自动检查所有数据库是否存在
- ✅ 为每个数据库生成独立的 GRANT 语句

---

## 📊 效率对比

| 场景 | v1.0 操作 | v2.0 操作 | 提升 |
|------|----------|----------|------|
| 创建单库账号 | 1 命令 | 1 命令 | - |
| 创建三库账号 | 1 命令 + 2 次 SQL | 1 命令 | **67%** |
| 添加 1 个数据库 | 手动 SQL | 1 命令 | **自动化** |
| 添加 3 个数据库 | 3 次 SQL | 1 命令 | **67%** |

---

## 🔍 使用场景

### 场景 1: 新项目上线，需要授权多个数据库

**v1.0 做法**:
```bash
# 创建用户
./create_readonly_account.sh -u admin -p pass -n reader -w pass123 -d app_db

# 手动添加其他数据库权限
mysql -u admin -p <<EOF
GRANT SHOW TABLES ON DATABASE analytics_db TO reader_role;
GRANT SELECT ON TABLE analytics_db.* TO reader_role;
GRANT SHOW TABLES ON DATABASE reports_db TO reader_role;
GRANT SELECT ON TABLE reports_db.* TO reader_role;
EOF
```

**v2.0 做法** (简化为 1 条命令):
```bash
./create_readonly_account.sh -u admin -p pass \
  -n reader -w pass123 \
  -d "app_db,analytics_db,reports_db"
```

### 场景 2: 已有只读账号，新增数据库后需要授权

**v1.0 做法** (手动 SQL):
```bash
mysql -u admin -p <<EOF
GRANT SHOW TABLES ON DATABASE new_db TO existing_role;
GRANT SELECT ON TABLE new_db.* TO existing_role;
EOF
```

**v2.0 做法** (自动化):
```bash
./create_readonly_account.sh -u admin -p pass \
  -n existing_user \
  --add-db new_db
```

### 场景 3: CI/CD 自动化部署

**v2.0 新增能力**:
```yaml
# 在部署脚本中自动添加新数据库权限
deploy:
  steps:
    - name: Create new database
      run: mysql -u admin -p < create_db.sql
    
    - name: Grant readonly access automatically
      run: |
        ./optools/create_readonly_account.sh \
          -u $ADMIN_USER -p $ADMIN_PASS \
          -n $READONLY_USER \
          --add-db $NEW_DATABASE
```

---

## 🧪 实际测试

### 测试 1: 添加单个数据库 ✅

```bash
$ ./create_readonly_account.sh -u dump -p 111 -n moi_readonly --add-db moi_main
[INFO] 数据库权限添加成功！
[INFO] 新增数据库: moi_main

$ echo "SELECT * FROM moi_main.test_table;" | mysql -umoi_readonly -p123
id      name
1       test1
2       test2
```

**结果**: ✅ 成功访问新数据库

### 测试 2: 批量添加多个数据库 ✅

```bash
$ ./create_readonly_account.sh -u dump -p 111 -n moi_readonly --add-db "test_db1,test_db2"
[INFO] 检查数据库 test_db1 是否存在... ✓
[INFO] 检查数据库 test_db2 是否存在... ✓
[INFO] 数据库权限添加成功！
```

**结果**: ✅ 同时添加多个数据库权限

### 测试 3: 创建多数据库账号 ✅

```bash
$ ./create_readonly_account.sh -u dump -p 111 \
    -n multi_db_reader -w pass123 \
    -d "moi,moi_main,test_db1"
[INFO] 只读账号创建成功！

$ mysql -umulti_db_reader -ppass123 <<EOF
SELECT COUNT(*) FROM moi.test_readonly;
SELECT COUNT(*) FROM moi_main.test_table;
EOF
3
2
```

**结果**: ✅ 可以访问所有授权的数据库

---

## 🏗️ 技术实现

### 双模式架构

```
用户输入参数
     ↓
检测是否有 --add-db
     ↓
  ┌─────┴─────┐
  ↓           ↓
创建模式    添加权限模式
  ↓           ↓
创建角色    验证用户存在
创建用户    验证角色存在
授予权限    添加数据库权限
  ↓           ↓
验证和测试  显示结果
```

### 关键代码

#### 1. 参数解析
```bash
case $1 in
    --add-db)
        ADD_DATABASES="$2"
        ADD_DATABASE_ONLY=1  # 切换到添加权限模式
        shift 2
        ;;
esac
```

#### 2. 数据库列表解析
```bash
# 解析逗号分隔的数据库列表
IFS=',' read -ra DB_ARRAY <<< "$DATABASE"
for db in "${DB_ARRAY[@]}"; do
    db=$(echo "$db" | xargs)  # 去除空格
    # 处理每个数据库
done
```

#### 3. SQL 生成（添加权限模式）
```bash
for db in "${DB_ARRAY[@]}"; do
    db=$(echo "$db" | xargs)
    echo "GRANT SHOW TABLES ON DATABASE ${db} TO ${ROLE_NAME};"
    echo "GRANT SELECT ON TABLE ${db}.* TO ${ROLE_NAME};"
done
```

---

## 📝 完整参数说明

| 参数 | 创建模式 | 添加权限模式 | 说明 |
|------|---------|-------------|------|
| `-h <host>` | 可选 | 可选 | 数据库主机 |
| `-P <port>` | 可选 | 可选 | 数据库端口 |
| `-u <user>` | **必需** | **必需** | 管理员用户名 |
| `-p <pass>` | **必需** | **必需** | 管理员密码 |
| `-n <name>` | 可选 | **必需** | 只读用户名 |
| `-w <pass>` | **必需** | ❌ 不需要 | 只读用户密码 |
| `-d <db>` | 可选 | ❌ 不使用 | 授权数据库列表 |
| `-r <role>` | 可选 | 可选 | 角色名 |
| `--add-db <db>` | ❌ 不使用 | **必需** | 要添加的数据库 |

---

## ⚠️ 注意事项

### 1. 权限缓存

添加新数据库权限后，**已登录的用户需要重新连接**才能使用新权限。

```bash
# 添加权限后
./create_readonly_account.sh -u admin -p pass -n reader --add-db new_db

# 用户需要重新连接
mysql -ureader -pXXX  # 退出并重新连接
```

### 2. 角色名推断

使用 `--add-db` 时，默认假定角色名为 `{username}_role`。如果实际角色名不同，需要用 `-r` 指定：

```bash
./create_readonly_account.sh -u admin -p pass \
  -n my_user \
  -r custom_role_name \  # 指定实际角色名
  --add-db new_db
```

### 3. 数据库名限制

- 支持逗号分隔多个数据库
- 不支持包含逗号的数据库名
- 自动去除数据库名前后的空格

---

## 🔄 向后兼容性

✅ **完全兼容** - v1.0 的所有用法在 v2.0 中保持不变

```bash
# v1.0 用法仍然有效
./create_readonly_account.sh -u admin -p pass -n reader -w pass123
./create_readonly_account.sh -u admin -p pass -n reader -w pass123 -d moi
```

---

## 📚 相关文档

- [详细使用说明](./README_create_readonly_account.md)
- [v2.0 完整更新日志](./create_readonly_account_CHANGELOG.md)
- [脚本内部实现分析](./create_readonly_account_ANALYSIS.md)
- [权限验证测试报告](./READONLY_ACCOUNT_TEST_RESULT.md)

---

## 🎉 总结

### v2.0 带来的价值

1. **效率提升 67%** - 多数据库场景操作次数大幅减少
2. **自动化增强** - 无需手动执行 SQL，降低出错风险
3. **灵活性提升** - 支持增量添加权限，适应动态环境
4. **完全兼容** - 无需修改现有脚本和流程

### 适用场景

- ✅ 多租户/多数据库环境
- ✅ 频繁新增数据库的环境
- ✅ CI/CD 自动化部署
- ✅ 标准化的权限管理流程

### 快速开始

```bash
# 创建多数据库账号
./create_readonly_account.sh -u admin -p pass \
  -n reader -w pass123 \
  -d "db1,db2,db3"

# 为已有账号添加数据库
./create_readonly_account.sh -u admin -p pass \
  -n reader \
  --add-db "new_db1,new_db2"
```

---

**版本**: v2.0  
**发布日期**: 2026-04-27  
**测试状态**: ✅ 所有功能已验证通过  
**向后兼容**: ✅ 完全兼容 v1.0
