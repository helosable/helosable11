from yoyo import step

_depends_ = {"20201226_002.py", "20201206_000_initial.py", "20201206_001.py"}

steps = [
    step("ALTER TABLE my_table ADD file_name VARCHAR(35) ")
]
