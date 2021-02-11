from yoyo import step

_depends_ = {"20201206_000_initial.py", "20201206_001.py"}

steps = [
    step("CREATE UNIQUE INDEX hash_unique_index ON my_table(row_hash)")
]
