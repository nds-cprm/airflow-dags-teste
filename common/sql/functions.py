def read_sql_from_file(filename) -> str:
    with open(filename, "r") as f:
        sql = f.read()

    return sql