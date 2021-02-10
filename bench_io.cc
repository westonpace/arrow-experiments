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

#include <chrono>
#include <iostream>

#ifdef _WIN32
#else

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#endif

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

using namespace arrow;

constexpr int64_t BLOCK_SIZE = 1024 * 1024;
constexpr int64_t NUM_FILES = 10;
constexpr int64_t NUM_TRIALS = 10;
constexpr int64_t NBYTES = 10 * 1024 * 1024;
constexpr int64_t TOTAL_BYTES = NBYTES * NUM_TRIALS * NUM_FILES;
constexpr char HDD_PREFIX[] = "/tmp/";
constexpr char SSD_PREFIX[] = "/media/pace/Extreme SSD3/arrow/";

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

Result<Future<int64_t>> ReadFileFullyAsync(int file_index, bool use_ssd,
                                           internal::Executor *io_executor) {
  const auto path = IndexToPath(file_index, use_ssd);
  ARROW_ASSIGN_OR_RAISE(auto file, io::ReadableFile::Open(path));
  ARROW_RETURN_NOT_OK(MarkNoCache(*file));
  ARROW_ASSIGN_OR_RAISE(auto it,
                        io::MakeInputStreamIterator(file, 1024 * 1024));
  ARROW_ASSIGN_OR_RAISE(auto gen,
                        MakeBackgroundGenerator(std::move(it), io_executor));

  struct Accumulator {
    Status operator()(std::shared_ptr<Buffer> buf) {
      bytes_read += buf->size();
      return Status::OK();
    }

    int64_t bytes_read = 0;
  };

  auto accumulator = std::make_shared<Accumulator>();
  std::function<Status(std::shared_ptr<Buffer> buf)> accumulator_fn =
      [accumulator](std::shared_ptr<Buffer> buf) {
        return (*accumulator)(std::move(buf));
      };
  return VisitAsyncGenerator(gen, accumulator_fn)
      .Then([file, accumulator](...) -> int64_t {
        DCHECK(10 * 1024 * 1024 == accumulator->bytes_read);
        return accumulator->bytes_read;
      });
}

Status ReadAllFiles(int nfiles, bool use_ssd, internal::Executor *io_executor) {
  std::vector<Future<int64_t>> file_readers;
  for (int i = 0; i < nfiles; i++) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          ReadFileFullyAsync(i, use_ssd, io_executor));
    file_readers.push_back(reader);
  }
  return All(file_readers).status();
}

Status ReadAllFilesSerially(int nfiles, bool use_ssd,
                            internal::Executor *io_executor) {
  auto counter = 0;
  AsyncGenerator<util::optional<int64_t>> generator =
      [&]() -> Future<util::optional<int64_t>> {
    if (counter == nfiles) {
      return Future<util::optional<int64_t>>::MakeFinished(
          IterationTraits<util::optional<int64_t>>::End());
    }
    auto index = counter++;
    auto result = ReadFileFullyAsync(index, use_ssd, io_executor);
    if (!result.ok()) {
      return Future<util::optional<int64_t>>::MakeFinished(result.status());
    }
    return result.ValueUnsafe().Then(
        [](const int64_t &bytes) { return util::optional<int64_t>(bytes); });
  };
  std::function<Status(util::optional<int64_t>)> visitor =
      [](util::optional<int64_t> nbytes) { return Status::OK(); };
  return VisitAsyncGenerator(generator, visitor).status();
}

Status RunMain(int argc, char **argv) {

  if (argc < 4) {
    return arrow::Status::Invalid(
        "Usage: ./bench_io ssd|not serial|not num_io_threads");
  }

  std::string use_ssd_option = argv[1];
  bool use_ssd = (use_ssd_option == "ssd");
  if (use_ssd) {
    std::cout << "Reading from SSD" << std::endl;
  } else {
    std::cout << "Reading from HDD" << std::endl;
  }

  std::string serial_read_option = argv[2];
  bool serial_read = (serial_read_option == "serial");
  if (serial_read) {
    std::cout << "Reading files serially" << std::endl;
  } else {
    std::cout << "Reading files in parallel" << std::endl;
  }

  std::string num_threads_option = argv[3];
  auto num_threads = std::atoi(num_threads_option.c_str());
  if (num_threads < 1) {
    return Status::Invalid("Invalid # of threads (" + num_threads_option + ")");
  }

  std::cout << "Creating test files" << std::endl;
  CreateSomeFiles(NUM_FILES, use_ssd);
  std::cout << "Reading files" << std::endl;
  ARROW_ASSIGN_OR_RAISE(auto io_executor,
                        internal::ThreadPool::Make(num_threads));
  auto start = high_resolution_clock::now();
  for (int i = 0; i < NUM_TRIALS; i++) {
    if (serial_read) {
      ReadAllFilesSerially(NUM_FILES, use_ssd, io_executor.get());
    } else {
      ReadAllFiles(NUM_FILES, use_ssd, io_executor.get());
    }
  }
  auto end = high_resolution_clock::now();

  auto bps =
      TOTAL_BYTES /
      static_cast<double>(duration_cast<nanoseconds>(end - start).count()) *
      1000000000.0;

  auto mbps = bps / 1000000;

  std::cout.imbue(std::locale(""));
  std::cout << "Elsaped: " << std::fixed
            << duration_cast<nanoseconds>(end - start).count() << " nanoseconds"
            << " MBPS=" << mbps << std::endl;

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
