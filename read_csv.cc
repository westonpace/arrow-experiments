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
#include <cassert>
#include <iostream>
#include <cstdlib>

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

using arrow::Status;

const int64_t BLOCK_SIZE = 4 * 1024 * 1024;

namespace {

  std::string SafeGetEnv(const std::string& name) {
    char * env = std::getenv(name.c_str());
    if(env == nullptr) {
      std::cerr << "You must define " << name << " environment variable" << std::endl;
      assert(false);
    }
    return env;
  }
  
Status RunMain(int argc, char **argv) {

  if (argc < 3) {
    return arrow::Status::Invalid(
        "You must specify a CSV directory to read and true/false for threads");
  }

  std::string csv_dir = argv[1];
  std::string use_threads_option = argv[2];
  bool use_threads = (use_threads_option == "true");
  if (use_threads) {
    std::cout << "Reading with AsyncTableReader" << std::endl;
  } else {
    std::cout << "Reading with ThreadedTableReader" << std::endl;
  }

  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.use_threads = use_threads;

  // auto thread_pool = arrow::internal::GetCpuThreadPool();
  // auto memory_pool = arrow::default_memory_pool();

  std::shared_ptr<arrow::fs::FileSystem> fs;
  if (csv_dir.substr(0, 5) == "s3://") {
    auto global_opts = arrow::fs::S3GlobalOptions();
    arrow::fs::InitializeS3(global_opts);
    auto s3_options = arrow::fs::S3Options::FromAccessKey(SafeGetEnv("AWS_ACCESS_KEY_ID"), SafeGetEnv("AWS_SECRET_ACCESS_KEY"));
    ARROW_ASSIGN_OR_RAISE(fs, arrow::fs::S3FileSystem::Make(s3_options));
    csv_dir = csv_dir.substr(5);
  } else {
    fs = std::make_shared<arrow::fs::LocalFileSystem>();
  }

  std::cout << "csv_dir: " << csv_dir << std::endl;

  auto selector = arrow::fs::FileSelector();
  selector.base_dir = csv_dir;
  selector.recursive = true;

  auto format = std::make_shared<arrow::dataset::CsvFileFormat>();

  
  auto fs_dataset_options = arrow::dataset::FileSystemFactoryOptions();
  fs_dataset_options.partitioning =
      arrow::dataset::HivePartitioning::MakeFactory();
  ARROW_ASSIGN_OR_RAISE(auto dataset_factory,
                        arrow::dataset::FileSystemDatasetFactory::Make(
                            fs, selector, format, fs_dataset_options));

  auto finish_options = arrow::dataset::FinishOptions();
  finish_options.validate_fragments = false;

  auto inspect_options = arrow::dataset::InspectOptions();
  inspect_options.fragments = 0;
  finish_options.inspect_options = inspect_options;
  ARROW_ASSIGN_OR_RAISE(auto dataset, dataset_factory->Finish(finish_options));

  int64_t total_duration = 0;
  for (int i = 0; i < 1; i++) {
    auto scan_options = std::make_shared<arrow::dataset::ScanOptions>();
    auto scanner_builder =
        arrow::dataset::ScannerBuilder(dataset);
    scanner_builder.UseThreads(true);
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder.Finish());

    auto start = high_resolution_clock::now();
    ARROW_ASSIGN_OR_RAISE(auto table, scanner->ToTable());
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<nanoseconds>(end - start).count();
    std::cout << "* Read table (" << table->num_rows() << ","
              << table->num_columns() << ")" << std::endl;
    std::cout.imbue(std::locale(""));
    std::cout << "Elsaped: " << std::fixed << duration << " nanoseconds"
              << std::endl;
    total_duration += duration;
  }

  std::cout << "Grand total: " << total_duration << " nanoseconds" << std::endl;

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
