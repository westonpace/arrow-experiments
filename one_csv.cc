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
#include <arrow/util/async_generator.h>
#include <arrow/util/iterator.h>
#include <arrow/util/thread_pool.h>

using arrow::Status;

const int64_t BLOCK_SIZE = 1 * 1024 * 1024;

namespace
{

  Status RunMain(int argc, char **argv)
  {

    if (argc < 2)
    {
      return arrow::Status::Invalid("Usage [csv_file]");
    }

    std::string csv_path = argv[1];

    bool use_streaming = false;
    bool use_threads = true;
    if (argc > 2)
    {
      std::string use_streaming_option = argv[2];
      use_streaming = use_streaming_option == "true";
    }
    if (argc > 3)
    {
      std::string use_threads_option = argv[3];
      std::cout << "use_threads_option=" << use_threads_option << std::endl;
      use_threads = use_threads_option != "false";
    }

    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    std::cout << "use_threads=" << use_threads << std::endl;
    read_options.use_threads = use_threads;

    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

    std::function<arrow::Result<std::shared_ptr<arrow::Table>>()> read_func;

    if (use_streaming)
    {
      read_func = [&]() -> arrow::Result<std::shared_ptr<arrow::Table>>
      {
        ARROW_ASSIGN_OR_RAISE(auto reader,
                              arrow::csv::StreamingReader::Make(
                                  arrow::io::default_io_context(),
                                  fs->OpenInputStream(csv_path).ValueOrDie(),
                                  read_options, parse_options, convert_options));
        std::shared_ptr<arrow::Table> table;
        ARROW_ASSIGN_OR_RAISE(table, reader->ToTable());
        return table;
      };
    }
    else
    {
      read_func = [&]() -> arrow::Result<std::shared_ptr<arrow::Table>>
      {
        ARROW_ASSIGN_OR_RAISE(auto reader,
                              arrow::csv::TableReader::Make(
                                  arrow::io::default_io_context(),
                                  fs->OpenInputStream(csv_path).ValueOrDie(),
                                  read_options, parse_options, convert_options));
        return reader->Read();
      };
    }

    int64_t total_count = 0;
    for (int i = 0; i < 1; i++)
    {
      auto start = high_resolution_clock::now();
      auto table = read_func().ValueOrDie();
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

int main(int argc, char **argv)
{
  Status st = RunMain(argc, argv);
  if (!st.ok())
  {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
