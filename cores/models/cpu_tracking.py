import psutil
from pandas import DataFrame
from front_end.utils.debug import format_table, print_table
from pathlib import Path

import time
from os import getpid

this_process_pid = getpid()

for m in psutil.process_iter():
    try:
        _ = m.cpu_percent()
    except (psutil.AccessDenied, psutil.ZombieProcess, psutil.NoSuchProcess):
        continue


def get_process_info(top_n_process: int = 10):
    process_info = []
    for proc in psutil.process_iter():
        try:
            process_info += [dict(
                PRIORITY=0,
                PID= proc.pid,
                NAME= proc.name()[:30],
                USER=proc.username(),
                CPU_PCT=proc.cpu_percent(),
                CPU_TIMES=round(sum(proc.cpu_times()) / 3600, 2),
                RAM_USED_PCT=proc.memory_percent(),
                RAM_USED_GB=round(proc.memory_info().rss / 1024 / 1024 / 1024, 2),
                CMD=(Path(proc.cmdline()[0]).name + (" " + proc.cmdline()[-1] if len(proc.cmdline()) > 1 else ""))[:50],
            )]
        except (psutil.AccessDenied, psutil.ZombieProcess, psutil.NoSuchProcess):
            continue

    total = [dict(
        PRIORITY=1,
        PID=None,
        NAME="ALL PROCESS",
        USER= None,
        CPU_PCT=psutil.cpu_percent(0.01),
        CPU_TIMES=None,
        RAM_USED_PCT=psutil.virtual_memory().percent,
        RAM_USED_GB=round(psutil.virtual_memory().used / 1024 / 1024 / 1024, 2),
        CMD=None,
    )]
    process_info += total

    df = DataFrame(process_info)
    df = df[df.PID.ne(this_process_pid)]
    df = df.sort_values(["PRIORITY", "CPU_PCT"], ascending=[False, False]).head(top_n_process + 1)
    df = df.drop(columns="PRIORITY")
    return df


if __name__ == "__main__":
    from curses import wrapper, delay_output

    def main(stdscr):
        while True:
            stdscr.clear()
            stdscr.refresh()

            data = format_table(get_process_info(), top_n=10, showindex=False)
            try:
                stdscr.addstr(0,0, data)
            except Exception as e:
                pass

            stdscr.refresh()
            # delay_output(1000)
            time.sleep(1)

    wrapper(main)

