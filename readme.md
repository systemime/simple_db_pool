## 简单数据库连接池

支持python版本：3.8+

依赖：
- mysqlclient

使用方法：
```python
import environs

from pool import ConnectionPool

env = environs.Env()
env.read_env(".env")


pool = ConnectionPool(
    env.str("MYSQL_DATABASE_NAME"),
    env.int("POOL_SIZE"),
    env.str("MYSQL_HOST"),
    env.int("MYSQL_PORT"),
    env.str("MYSQL_USER"),
    env.str("MYSQL_ROOT_PASSWORD"),
)

# 事务支持
with pool.transaction_context() as db:
    rows = db.query("SELECT * FROM `xxx` LIMIT 1")
    print(rows)

# 普通链接
with pool.connect() as db1:
    rows = db1.query("SELECT `id` FROM `xxx` LIMIT 1")
    print(rows)
```
