def log_tests(log, issql=False, header=False, last_in_group=False, new_log_file=False):
    log = log.strip()
    with open("Test_Log.md", "w" if new_log_file else "a") as file:
        if issql:
            file.write(f"\n```sql\n{log}\n```\n\n")
        elif header:
            file.write(f"### {log} ### \n")
        elif last_in_group:
            file.write(f"{log}\n\n\n")
        else:
            file.write(f"{log} <br />")


def save_output(result):
    with open("Query_Output.md", "w") as file:
        file.write(result)
