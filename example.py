import environs

from pool import ConnectionPool

env = environs.Env()
env.read_env(".env")


if __name__ == "__main__":

    pool = ConnectionPool(
        env.str("MYSQL_DATABASE_NAME_system"),
        env.int("max_num"),
        env.str("MYSQL_HOST_system"),
        env.int("MYSQL_PORT_system"),
        env.str("MYSQL_USER_system"),
        env.str("MYSQL_ROOT_PASSWORD_system"),
    )

    while True:
        with pool.transaction_context() as db:
            rows = db.query("SELECT * FROM `xxx` LIMIT 1")
            print(rows)

        with pool.connect() as db1:
            rows = db1.query("SELECT `id` FROM `xxx` LIMIT 1")
            print(rows)
        import time

        time.sleep(1)
