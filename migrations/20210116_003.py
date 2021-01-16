from yoyo import step

_depends_ = {"20201206_000_initial.py"}

steps = [
    step("ALTER TABLE my_table ADD from_where VARCHAR(35) ")
]
