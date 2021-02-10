import os
import signal
import subprocess
import sys
import tempfile
import time

import numpy.random

NBYTES = 1024 * 1024 * 10
NUM_ITERATIONS = 10
NUM_FILES = 10


def index_to_path(use_ssd, file_index):
    if use_ssd:
        return f"/media/pace/Extreme SSD3/arrow/{file_index}.dat"
    else:
        return f"/tmp/{file_index}.dat"


def get_disk(use_ssd):
    if use_ssd:
        return "/dev/sdb"
    else:
        return "/dev/sda6"


def get_disk_short(use_ssd):
    return get_disk(use_ssd).split("/")[2]


def uncache(path):
    fd = os.open(path, os.O_RDWR)
    os.fdatasync(fd)
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    os.close(fd)


def generate_file(use_ssd, file_index, data):
    path = index_to_path(use_ssd, file_index)
    with open(path, "wb") as f:
        f.write(data)
    uncache(path)


def generate_files(use_ssd, num_files):
    data = numpy.random.bytes(NBYTES)
    for file_index in range(num_files):
        generate_file(use_ssd, file_index, data)


def start_blktrace(use_ssd):
    blktrace_dir = tempfile.TemporaryDirectory()
    blktrace_proc = subprocess.Popen(
        f"blktrace -d {get_disk(use_ssd)} -w 15",
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        cwd=blktrace_dir.name,
        shell=True,
    )
    return (blktrace_dir, blktrace_proc)


def run_bench_io(use_ssd, read_serially, num_threads):
    ssd_str = "ssd" if use_ssd else "hdd"
    serial_str = "serial" if read_serially else "parallel"
    cmd = f"build/bench_io {ssd_str} {serial_str} {num_threads}"
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    interesting_line = proc.stdout.split("\n")[4]
    fields = interesting_line.split("=")
    return float(fields[1])


def parse_blktrace_output(output):
    last = None
    hits = 0
    misses = 0
    for line in output.split("\n"):
        tokens = line.split()
        if len(tokens) > 9:
            action = tokens[5]
            if action == "D":
                start = int(tokens[7])
                end = int(tokens[9])
                if last is None or start == last:
                    hits += 1
                else:
                    misses += 1
                last = start + end
    return (hits, misses)


def parse_blktrace(use_ssd, blktrace_handle):
    blktrace_dir, blktrace_proc = blktrace_handle
    blktrace_proc.wait(20)
    cmd = f"blkparse {blktrace_dir.name}/{get_disk_short(use_ssd)}"
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    blktrace_dir.cleanup()
    if proc.returncode != 0:
        return 0, 0
    return parse_blktrace_output(proc.stdout)


def run_bench_io_trial(use_ssd, read_serially, num_threads):
    blktrace_handle = start_blktrace(use_ssd)
    mbps = run_bench_io(use_ssd, read_serially, num_threads)
    (hits, misses) = parse_blktrace(use_ssd, blktrace_handle)
    use_ssd_str = "1" if use_ssd else "0"
    read_serially_str = "1" if read_serially else "0"
    print(f"{mbps},{hits},{misses},{use_ssd_str},{read_serially_str},{num_threads}")


def run_bench_io_trials(use_ssd, read_serially, num_threads):
    runs = []
    for _ in range(NUM_ITERATIONS):
        runs.append(run_bench_io_trial(use_ssd, read_serially, num_threads))
    return runs


if __name__ == "__main__":
    generate_files(True, NUM_FILES)
    generate_files(False, NUM_FILES)
    print("mbps,seqreads,rndreads,use_ssd,serial,io_threads")
    run_bench_io_trials(True, True, 1)
    run_bench_io_trials(True, False, 1)
    run_bench_io_trials(False, True, 1)
    run_bench_io_trials(False, False, 1)
    run_bench_io_trials(True, True, 8)
    run_bench_io_trials(True, False, 8)
    run_bench_io_trials(False, True, 8)
    run_bench_io_trials(False, False, 8)
