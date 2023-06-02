// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include </usr/include/papi.h>
#include <cassert>
#include <chrono>
#include <fcntl.h>
#include <iostream>
#include <poll.h>
#include <pthread.h>
#include <thread>
#include <unistd.h>

using namespace std::chrono;

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/io/compressed.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/io_util.h>
#include <arrow/util/iterator.h>
#include <arrow/util/logging.h>
#include <arrow/util/macros.h>

using namespace arrow;

constexpr int64_t NUM_FILES = 1;
constexpr int64_t NUM_TRIALS = 10;
constexpr int64_t TASK_MULTIPLIER = 30;
constexpr int64_t NBYTES = 100 * 1024 * 1024;
constexpr int64_t TOTAL_BYTES = NBYTES * NUM_TRIALS * NUM_FILES;
constexpr char HDD_PREFIX[] = "/tmp/";
constexpr char SSD_PREFIX[] = "/media/pace/Extreme SSD3/arrow/";

#ifdef REDO_MACROS
#define ARROW_EXPAND(x) x
#define ARROW_STRINGIFY(x) #x
#define ARROW_CONCAT(x, y) x##y

#define ARROW_PREDICT_FALSE(x) (__builtin_expect(!!(x), 0))
#define ARROW_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#define ARROW_NORETURN __attribute__((noreturn))
#define ARROW_NOINLINE __attribute__((noinline))
#define ARROW_PREFETCH(addr) __builtin_prefetch(addr)

#define ARROW_RETURN_IF_(condition, status, _)                                 \
  do {                                                                         \
    if (ARROW_PREDICT_FALSE(condition)) {                                      \
      return (status);                                                         \
    }                                                                          \
  } while (0)

#define ARROW_RETURN_IF(condition, status)                                     \
  ARROW_RETURN_IF_(condition, status, ARROW_STRINGIFY(status))

/// \brief Propagate any non-successful Status to the caller
#define ARROW_RETURN_NOT_OK(status)                                            \
  do {                                                                         \
    ::arrow::Status __s = ::arrow::internal::GenericToStatus(status);          \
    ARROW_RETURN_IF_(!__s.ok(), __s, ARROW_STRINGIFY(status));                 \
  } while (false)

#define RETURN_NOT_OK_ELSE(s, else_)                                           \
  do {                                                                         \
    ::arrow::Status _s = ::arrow::internal::GenericToStatus(s);                \
    if (!_s.ok()) {                                                            \
      else_;                                                                   \
      return _s;                                                               \
    }                                                                          \
  } while (false)

// This is an internal-use macro and should not be used in public headers.
#define RETURN_NOT_OK(s) ARROW_RETURN_NOT_OK(s)

#define ARROW_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr)                    \
  auto &&result_name = (rexpr);                                                \
  ARROW_RETURN_IF_(!(result_name).ok(), (result_name).status(),                \
                   ARROW_STRINGIFY(rexpr));                                    \
  lhs = std::move(result_name).ValueUnsafe();

#define ARROW_ASSIGN_OR_RAISE_NAME(x, y) ARROW_CONCAT(x, y)

#define ARROW_ASSIGN_OR_RAISE(lhs, rexpr)                                      \
  ARROW_ASSIGN_OR_RAISE_IMPL(                                                  \
      ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);
#endif

namespace {

static Status Uncache(std::string path) {
#ifdef _WIN32
#else
  auto fd = open(path.c_str(), O_RDONLY);

  auto err = fdatasync(fd);
  Status st = Status::OK();
  if (err) {
    st = internal::IOErrorFromErrno(err, "Could not call fdatasync on fd");
  } else {
    err = posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    if (err) {
      st =
          internal::IOErrorFromErrno(err, "Could not call posix_fadvise on fd");
    }
  }
  err = close(fd);
  if (err) {
    if (!st.ok()) {
      return st;
    } else {
      return internal::IOErrorFromErrno(err, "Could not call close on fd");
    }
  }
  if (!st.ok()) {
    return st;
  }
#endif
  return Status::OK();
}

static Status MarkNoCache(const io::ReadableFile &file) {
#ifdef _WIN32
#else
  auto err = posix_fadvise(file.file_descriptor(), 0, 0, POSIX_FADV_DONTNEED);
  if (err) {
    return internal::IOErrorFromErrno(
        err, "Could not call posix_fadvise on existing fd");
  }
#endif
  return Status::OK();
}

std::string IndexToPath(int file_index, bool use_ssd) {
  if (use_ssd) {
    return SSD_PREFIX + std::to_string(file_index) + ".dat";
  } else {
    return HDD_PREFIX + std::to_string(file_index) + ".dat";
  }
}

Status CreateFile(int file_index, bool use_ssd, const void *data) {
  const auto path = IndexToPath(file_index, use_ssd);
  ARROW_ASSIGN_OR_RAISE(auto file, io::FileOutputStream::Open(path));
  ARROW_RETURN_NOT_OK(file->Write(data, NBYTES));
  ARROW_RETURN_NOT_OK(file->Close());
  ARROW_RETURN_NOT_OK(Uncache(path));
  return Status::OK();
}

void CreateSomeFiles(int num_files, bool use_ssd) {
  const std::string datastr(NBYTES, 'x');
  const void *data = datastr.data();
  for (auto i = 0; i < num_files; i++) {
    CreateFile(i, use_ssd, data);
  }
}

Status UncacheFiles(int num_files, bool use_ssd) {
  for (auto i = 0; i < num_files; i++) {
    RETURN_NOT_OK(Uncache(IndexToPath(i, use_ssd)));
  }
  return Status::OK();
}

Result<int64_t> ReadFileFullyAsync(int file_index, bool use_ssd,
                                   bool examine_file, bool delay,
                                   int block_size_kb, int blocks_readahead,
                                   internal::Executor *io_executor,
                                   internal::Executor *cpu_executor) {
  const auto path = IndexToPath(file_index, use_ssd);
  ARROW_ASSIGN_OR_RAISE(auto file, io::ReadableFile::Open(path));
  ARROW_RETURN_NOT_OK(MarkNoCache(*file));
  ARROW_ASSIGN_OR_RAISE(
      auto it, io::MakeInputStreamIterator(file, block_size_kb * 1024));
  ARROW_ASSIGN_OR_RAISE(auto gen, MakeBackgroundGenerator(
                                      std::move(it), io_executor,
                                      blocks_readahead, blocks_readahead / 2));
  gen = MakeTransferredGenerator(std::move(gen), cpu_executor);

  struct Accumulator {
    Accumulator(bool examine_file, bool delay)
        : examine_file(examine_file), delay(delay), bytes_read(0), unused(0) {}

    Status operator()(std::shared_ptr<Buffer> buf) {
      if (examine_file) {
        for (int i = 0; i < TASK_MULTIPLIER; i++) {
          auto data = buf->data();
          for (int64_t i = 0; i < buf->size(); i++) {
            unused += data[i];
          }
        }
      }
      bytes_read += buf->size();
      return Status::OK();
    }

    bool examine_file;
    bool delay;
    int64_t bytes_read;
    int64_t unused;
  };

  auto accumulator = std::make_shared<Accumulator>(examine_file, delay);
  std::function<Status(std::shared_ptr<Buffer> buf)> accumulator_fn =
      [accumulator](std::shared_ptr<Buffer> buf) {
        return (*accumulator)(std::move(buf));
      };
  return VisitAsyncGenerator(gen, accumulator_fn)
      .Then([file, accumulator](...) -> int64_t {
        std::cout << "NBYTES=" << NBYTES
                  << " bytes_read=" << accumulator->bytes_read << std::endl;
        assert(NBYTES == accumulator->bytes_read);
        return accumulator->bytes_read;
      })
      .result();
}

Result<int64_t> ReadFileFullySerial(int file_index, bool use_ssd,
                                    bool examine_file, bool delay,
                                    int block_size_kb) {
  const auto path = IndexToPath(file_index, use_ssd);
  ARROW_ASSIGN_OR_RAISE(auto file, io::ReadableFile::Open(path));
  ARROW_RETURN_NOT_OK(MarkNoCache(*file));
  ARROW_ASSIGN_OR_RAISE(
      auto it, io::MakeInputStreamIterator(file, block_size_kb * 1024));

  struct Accumulator {
    Accumulator(bool examine_file, bool delay)
        : examine_file(examine_file), delay(delay), bytes_read(0), unused(0) {}

    Status operator()(std::shared_ptr<Buffer> buf) {
      if (examine_file) {
        auto data = buf->data();
        for (int64_t i = 0; i < buf->size(); i++) {
          unused += data[i];
        }
      }
      bytes_read += buf->size();
      return Status::OK();
    }

    bool examine_file;
    bool delay;
    int64_t bytes_read;
    int64_t unused;
  };

  auto accumulator = std::make_shared<Accumulator>(examine_file, delay);
  std::function<Status(std::shared_ptr<Buffer> buf)> accumulator_fn =
      [accumulator](std::shared_ptr<Buffer> buf) {
        return (*accumulator)(std::move(buf));
      };
  it.Visit(accumulator_fn);

  std::cout << "NBYTES=" << NBYTES << " bytes_read=" << accumulator->bytes_read
            << std::endl;
  assert(NBYTES == accumulator->bytes_read);
  return accumulator->bytes_read;
}

Status RunMain(int argc, char **argv) {

  if (argc < 9) {
    return arrow::Status::Invalid(
        "Usage: ./bench_io log|no_log ssd|not serial|not examine|no_examine "
        "delay|no_delay num_io_threads block_size_kb blocks_readahead");
  }

  std::string should_log_option = argv[1];
  bool should_log = (should_log_option == "log");

  std::string use_ssd_option = argv[2];
  bool use_ssd = (use_ssd_option == "ssd");
  if (use_ssd) {
    std::cout << "Reading from SSD" << std::endl;
  } else {
    std::cout << "Reading from HDD" << std::endl;
  }

  std::string serial_read_option = argv[3];
  bool serial_read = (serial_read_option == "serial");
  if (serial_read) {
    std::cout << "Reading files serially" << std::endl;
  } else {
    std::cout << "Reading files in parallel" << std::endl;
  }

  std::string examine_file_option = argv[4];
  bool examine_file = (examine_file_option == "examine");

  std::string delay_option = argv[5];
  bool delay = (delay_option == "delay");

  std::string num_threads_option = argv[6];
  auto num_threads = std::atoi(num_threads_option.c_str());
  if (num_threads < 1) {
    return Status::Invalid("Invalid # of threads (" + num_threads_option + ")");
  }

  std::string block_size_kb_option = argv[7];
  auto block_size_kb = std::atoi(block_size_kb_option.c_str());
  if (block_size_kb < 1) {
    return Status::Invalid("Invalid block size (" + block_size_kb_option + ")");
  }

  std::string blocks_readahead_option = argv[6];
  auto blocks_readahead = std::atoi(blocks_readahead_option.c_str());
  if (blocks_readahead < 1) {
    return Status::Invalid("Invalid block readahead (" +
                           blocks_readahead_option + ")");
  }

  if (should_log) {
    std::cout << "Enabling logging" << std::endl;
    arrow::util::ArrowLog::StartArrowLog(
        "ReadCsv", arrow::util::ArrowLogLevel::ARROW_DEBUG);
  }

  std::cout << "Creating test files" << std::endl;
  CreateSomeFiles(NUM_FILES, use_ssd);

  std::vector<std::string> events = {"PAPI_TOT_INS", "perf::CACHE-MISSES",
                                     "perf::TASK-CLOCK"};
  int num_events = static_cast<int>(events.size());

  int retval, EventSet = PAPI_NULL;
  long_long values[NUM_TRIALS][num_threads + num_threads][events.size()];
  for (int i = 0; i < NUM_TRIALS; i++) {
    for (int j = 0; j < num_threads * 2; j++) {
      for (int k = 0; k < events.size(); k++) {
        values[i][j][k] = 0;
      }
    }
  }
  retval = PAPI_library_init(PAPI_VER_CURRENT);
  if (retval != PAPI_VER_CURRENT) {
    return Status::Invalid("Invalid PAPI version");
  }

  if (PAPI_thread_init(pthread_self) != PAPI_OK) {
    return Status::Invalid("Could not init threads");
  }

  if (PAPI_create_eventset(&EventSet) != PAPI_OK) {
    return Status::Invalid("Could not create event set");
  }

  for (const auto &event : events) {
    if (PAPI_add_named_event(EventSet, event.c_str()) != PAPI_OK) {
      return Status::Invalid("Could not add event ", event);
    }
  }

  if (PAPI_start(EventSet) != PAPI_OK) {
    return Status::Invalid("Could not start PAPI");
  }

  int64_t trial_times[NUM_TRIALS];
  for (int i = 0; i < NUM_TRIALS; i++) {

    auto start = high_resolution_clock::now();

    std::atomic<int> counter(0);
    auto trial_values = values[i];

    std::function<int()> thread_start = [&events] {
      int EventSet = PAPI_NULL;
      if (PAPI_register_thread() != PAPI_OK) {
        std::cerr << "Could not register thread" << std::endl;
      }

      if (PAPI_create_eventset(&EventSet) != PAPI_OK) {
        std::cout << "Could not create event set" << std::endl;
      }

      for (const auto &event : events) {
        if (PAPI_add_named_event(EventSet, event.c_str()) != PAPI_OK) {
          std::cout << "Could not add event " << event << std::endl;
        }
      }

      if (PAPI_start(EventSet) != PAPI_OK) {
        std::cout << "Could not start PAPI" << std::endl;
      }
      return EventSet;
    };

    std::function<int()> io_thread_start = [&events] {
      sched_param param;
      param.sched_priority = 1;
      // pthread_setschedparam(pthread_self(), SCHED_FIFO, &param);
      int EventSet = PAPI_NULL;
      if (PAPI_register_thread() != PAPI_OK) {
        std::cerr << "Could not register thread" << std::endl;
      }

      if (PAPI_create_eventset(&EventSet) != PAPI_OK) {
        std::cout << "Could not create event set" << std::endl;
      }

      for (const auto &event : events) {
        if (PAPI_add_named_event(EventSet, event.c_str()) != PAPI_OK) {
          std::cout << "Could not add event " << event << std::endl;
        }
      }

      if (PAPI_start(EventSet) != PAPI_OK) {
        std::cout << "Could not start PAPI" << std::endl;
      }
      return EventSet;
    };

    long_long *values_ptr = (long_long *)values;

    std::function<void(int)> thread_stop = [values_ptr, i, num_threads,
                                            num_events, &counter,
                                            &events](int EventSet) {
      int values_id = counter++;

      long_long *task_events = values_ptr + (i * 2 * num_threads * num_events) +
                               (values_id * num_events);
      if (PAPI_read(EventSet, task_events) != PAPI_OK) {
        std::cout << "Could not read events for thread" << std::endl;
      }

      if (PAPI_unregister_thread() != PAPI_OK) {
        std::cout << "Could not unregister thread" << std::endl;
      }
    };

    ARROW_ASSIGN_OR_RAISE(
        auto io_executor,
        internal::ThreadPool::Make(num_threads, thread_start, thread_stop));
    ARROW_ASSIGN_OR_RAISE(
        auto cpu_executor,
        internal::ThreadPool::Make(num_threads, thread_start, thread_stop));

    if (PAPI_reset(EventSet) != PAPI_OK) {
      return Status::Invalid("Could not reset PAPI");
    }
    if (serial_read) {
      ARROW_RETURN_NOT_OK(
          ReadFileFullySerial(0, use_ssd, examine_file, delay, block_size_kb));
    } else {
      ReadFileFullyAsync(0, use_ssd, examine_file, delay, block_size_kb,
                         blocks_readahead, io_executor.get(),
                         cpu_executor.get());
    }

    auto end = high_resolution_clock::now();
    trial_times[i] = duration_cast<nanoseconds>(end - start).count();

    std::cout << "  I/O tasks " << io_executor->GetTotalTasksQueued()
              << std::endl;
    std::cout << "  I/O capacity " << io_executor->GetActualCapacity()
              << std::endl;
    std::cout << "  CPU tasks " << cpu_executor->GetTotalTasksQueued()
              << std::endl;
    std::cout << "  CPU capacity " << cpu_executor->GetActualCapacity()
              << std::endl;
  }

  long_long main_values[events.size()];
  if (PAPI_read(EventSet, main_values) != PAPI_OK) {
    std::cout << "Could not read events for main thread" << std::endl;
  }

  double totals[events.size()];
  for (std::size_t i = 0; i < events.size(); i++) {
    totals[i] = 0;
  }

  for (std::size_t i = 0; i < events.size(); i++) {
    for (int t = 0; t < num_threads * 2; t++) {
      for (int j = 0; j < NUM_TRIALS; j++) {
        totals[i] += values[j][t][i];
      }
    }
  }

  int64_t total_trial_time_nanos = 0;
  for (int i = 0; i < NUM_TRIALS; i++) {
    total_trial_time_nanos += trial_times[i];
  }

  double mean_trial_time =
      static_cast<double>(total_trial_time_nanos) / NUM_TRIALS;

  double total_error = 0;
  for (int i = 0; i < NUM_TRIALS; i++) {
    total_error += fabs(trial_times[i] - mean_trial_time);
  }

  auto bps =
      TOTAL_BYTES / static_cast<double>(total_trial_time_nanos) * 1000000000.0;

  auto mbps = bps / 1000000;

  std::cout.imbue(std::locale(""));
  std::cout << "Elsaped: " << std::fixed << total_trial_time_nanos
            << " nanoseconds"
            << " MBPS=" << mbps << std::endl
            << std::endl;

  std::cout << "TOTAL NANOS: " << total_trial_time_nanos << std::endl;
  std::cout << "STDERR NANOS: " << (total_error / NUM_TRIALS) << std::endl
            << std::endl;

  for (std::size_t i = 0; i < events.size(); i++) {
    std::cout << events[i] << ": " << (totals[i] / NUM_TRIALS) << std::endl;
    double mean = totals[i] / NUM_TRIALS;
    double error = 0;
    for (int j = 0; j < NUM_TRIALS; j++) {
      double trial_sum = 0;
      for (int t = 0; t < num_threads * 2; t++) {
        trial_sum += values[j][t][i];
      }
      error += fabs(trial_sum - mean);
    }
    std::cout << "  Std. Error: " << (error / NUM_TRIALS) << std::endl;
  }

  std::cout << std::endl;

  for (std::size_t i = 0; i < events.size(); i++) {
    std::cout << events[i] << ": "
              << (static_cast<double>(main_values[i]) / NUM_TRIALS)
              << std::endl;
  }

  if (should_log) {
    arrow::util::ArrowLog::ShutDownArrowLog();
  }

  return Status::OK();
}

} // namespace

int main(int argc, char **argv) {
  Status st = RunMain(argc, argv);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
