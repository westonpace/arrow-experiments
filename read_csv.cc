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
#include <arrow/io/api.h>
#include <arrow/io/compressed.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/async_iterator.h>
#include <arrow/util/iterator.h>

using arrow::Status;

const int64_t BLOCK_SIZE = 1024 * 1024;

namespace {

Status RunMain(int argc, char **argv) {

  if (argc < 3) {
    return arrow::Status::Invalid(
        "You must specify a CSV file to read and true/false for async");
  }

  std::string csv_filename = argv[1];
  std::string async_option = argv[2];
  bool is_async = (async_option == "true");
  if (is_async) {
    std::cout << "Reading with AsyncTableReader" << std::endl;
  } else {
    std::cout << "Reading with ThreadedTableReader" << std::endl;
  }

  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.use_threads = true;

  auto parse_options = arrow::csv::ParseOptions::Defaults();
  parse_options.delimiter = '|';

  auto thread_pool = arrow::internal::GetCpuThreadPool();
  auto memory_pool = arrow::default_memory_pool();

  std::shared_ptr<arrow::io::InputStream> input_stream;
  ARROW_ASSIGN_OR_RAISE(input_stream,
                        arrow::io::ReadableFile::Open(csv_filename));

  auto gzip_codec =
      arrow::util::Codec::Create(arrow::Compression::GZIP).ValueOrDie();

  if (csv_filename[csv_filename.size() - 1] == 'z') {
    input_stream =
        arrow::io::CompressedInputStream::Make(gzip_codec.get(), input_stream)
            .ValueOrDie();
  }

  std::shared_ptr<arrow::csv::TableReader> table_reader;
  ARROW_ASSIGN_OR_RAISE(
      table_reader,
      arrow::csv::TableReader::Make(
          arrow::default_memory_pool(), arrow::io::AsyncContext(), input_stream,
          read_options, parse_options, arrow::csv::ConvertOptions::Defaults()));

  std::cout << "* reading CSV file '" << csv_filename << "' into table"
            << std::endl;

  auto start = high_resolution_clock::now();
  ARROW_ASSIGN_OR_RAISE(auto table, table_reader->Read());
  auto end = high_resolution_clock::now();

  std::cout << "* Read table (" << table->num_rows() << ","
            << table->num_columns() << ")" << std::endl;
  std::cout.imbue(std::locale(""));
  std::cout << "Elsaped: " << std::fixed
            << duration_cast<nanoseconds>(end - start).count() << " nanoseconds"
            << std::endl;

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
