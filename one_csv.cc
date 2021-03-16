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

using namespace std::chrono;

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/io/compressed.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/iterator.h>
#include <arrow/util/thread_pool.h>

using arrow::Status;

const int64_t BLOCK_SIZE = 1 * 1024 * 1024;

namespace {

Status RunMain(int argc, char **argv) {

  if (argc < 4) {
    return arrow::Status::Invalid(
        "Usage [csv_file] [use_threads] [use_async] [num_threads]");
  }

  std::string csv_path = argv[1];
  std::string use_threads_option = argv[2];
  std::string use_streaming_option = argv[3];
  std::string num_threads_option = argv[4];
  bool use_threads = (use_threads_option == "true");
  bool use_streaming = (use_streaming_option == "true");
  if (use_threads && !use_streaming) {
    std::cout << "Reading with AsyncTableReader" << std::endl;
  } else if (!use_streaming) {
    std::cout << "Reading with SerialTableReader" << std::endl;
  } else {
    std::cout << "Reading with StreamingTableReader" << std::endl;
  }
  int num_threads = std::atoi(num_threads_option.c_str());

  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.use_threads = use_threads;
  read_options.block_size = BLOCK_SIZE;

  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

  arrow::SetCpuThreadPoolCapacity(num_threads);

  std::function<std::shared_ptr<arrow::Table>()> read_func;
  if (use_streaming) {
    read_func = [read_options, fs, csv_path]() {
      auto input = fs->OpenInputStream(csv_path).ValueOrDie();
      auto reader =
          arrow::csv::StreamingReader::Make(
              arrow::io::default_io_context().pool(), input, read_options,
              arrow::csv::ParseOptions(), arrow::csv::ConvertOptions())
              .ValueOrDie();
      std::shared_ptr<arrow::Table> out;
      if (!reader->ReadAll(&out).ok()) {
        abort();
      };
      return out;
    };
  } else {
    read_func = [read_options, fs, csv_path]() {
      auto input = fs->OpenInputStream(csv_path).ValueOrDie();
      auto reader =
          arrow::csv::TableReader::Make(
              arrow::io::default_io_context(), input, read_options,
              arrow::csv::ParseOptions(), arrow::csv::ConvertOptions())
              .ValueOrDie();
      return reader->Read().ValueOrDie();
    };
  }

  int64_t total_count = 0;
  for (int i = 0; i < 1; i++) {
    auto start = high_resolution_clock::now();
    auto table = read_func();
    auto end = high_resolution_clock::now();

    auto duration = duration_cast<nanoseconds>(end - start).count();
    total_count += duration;

    std::cout << "* Read table (" << table->num_rows() << ","
              << table->num_columns() << ")" << std::endl;
    std::cout.imbue(std::locale(""));
    std::cout << "Elsaped: " << std::fixed << duration << " nanoseconds"
              << std::endl;
  }
  std::cout << "Total duration " << std::fixed << total_count << " nanoseconds"
            << std::endl;
  // std::cout << "Real futures created: "
  //           << arrow::FutureCounters::real_futures_created.load() <<
  //           std::endl;
  // std::cout << "Finished futures created: "
  //           << arrow::FutureCounters::finished_futures_created.load()
  //           << std::endl;

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
