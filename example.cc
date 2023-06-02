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

#include <arrow/acero/api.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/plan.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/api.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/api.h>
#include <arrow/io/interfaces.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type_fwd.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>
#include <queue>
#include <thread>

namespace {

class ProxyRandomAccessFile : public arrow::io::RandomAccessFile {
public:
  ProxyRandomAccessFile(std::shared_ptr<arrow::io::RandomAccessFile> target)
      : target_(std::move(target)) {}

  arrow::Result<int64_t> Read(int64_t nbytes, void *out) override {
    std::cout << "Read(" << nbytes << ")" << std::endl;
    bytes_read_ += nbytes;
    return target_->Read(nbytes, out);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    std::cout << "Read(" << nbytes << ")" << std::endl;
    bytes_read_ += nbytes;
    return target_->Read(nbytes);
  }

  arrow::Status Seek(int64_t position) override {
    std::cout << "Seek(" << position << ")" << std::endl;
    return target_->Seek(position);
  }

  arrow::Result<std::string_view> Peek(int64_t nbytes) override {
    // This is technically a read but, to avoid double counting, we will assume
    // these bytes are read later
    std::cout << "Peek(" << nbytes << ")" << std::endl;
    return target_->Peek(nbytes);
  }

  arrow::Result<int64_t> GetSize() override { return target_->GetSize(); }

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes,
                                void *out) override {
    std::cout << "ReadAt(" << position << "," << nbytes << ")" << std::endl;
    bytes_read_ += nbytes;
    return target_->ReadAt(position, nbytes, out);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>>
  ReadAt(int64_t position, int64_t nbytes) override {
    std::cout << "ReadAt(" << position << "," << nbytes << ")" << std::endl;
    bytes_read_ += nbytes;
    return target_->ReadAt(position, nbytes);
  }

  arrow::Future<std::shared_ptr<arrow::Buffer>>
  ReadAsync(const arrow::io::IOContext &ctx, int64_t position,
            int64_t nbytes) override {
    std::cout << "ReadAsync(" << position << "," << nbytes << ")" << std::endl;
    bytes_read_ += nbytes;
    return target_->ReadAsync(ctx, position, nbytes);
  }

  std::vector<arrow::Future<std::shared_ptr<arrow::Buffer>>>
  ReadManyAsync(const arrow::io::IOContext &io_ctx,
                const std::vector<arrow::io::ReadRange> &ranges) override {
    std::cout << "ReadMany!" << std::endl;
    for (const auto &range : ranges) {
      bytes_read_ += range.length;
    }
    return target_->ReadManyAsync(io_ctx, std::move(ranges));
  }

  arrow::Status
  WillNeed(const std::vector<arrow::io::ReadRange> &ranges) override {
    return target_->WillNeed(std::move(ranges));
  }

  arrow::Status Close() override { return target_->Close(); }

  arrow::Future<> CloseAsync() override { return target_->CloseAsync(); }

  arrow::Status Abort() override { return target_->Abort(); }

  arrow::Result<int64_t> Tell() const override { return target_->Tell(); }

  bool closed() const override { return target_->closed(); }

  int64_t bytes_read() { return bytes_read_; }

  void ResetBytesRead() { bytes_read_ = 0; }

private:
  std::shared_ptr<arrow::io::RandomAccessFile> target_;
  int64_t bytes_read_ = 0;
};

// Utility functions for converting arrow::Result & arrow::Status into
// exceptions
template <typename T> T throw_or_assign(arrow::Result<T> value) {
  if (!value.ok()) {
    throw std::runtime_error(value.status().ToString());
  }
  return value.MoveValueUnsafe();
}

void throw_not_ok(const arrow::Status &status) {
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
}

void RunTest() {
  arrow::dataset::internal::Initialize();
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

  auto parquet_format = std::make_shared<arrow::dataset::ParquetFileFormat>();

  auto factory = throw_or_assign(arrow::dataset::FileSystemDatasetFactory::Make(
      "file:///home/pace/dev/data/ideal", parquet_format, {}));

  auto dataset = throw_or_assign(factory->Finish());

  // arrow::dataset::ScanV2Options scan_options(dataset);
  // scan_options.columns =
  //     arrow::dataset::ScanV2Options::AllColumns(*dataset->schema());

  auto scan_options = std::make_shared<arrow::dataset::ScanOptions>();
  scan_options->dataset_schema = dataset->schema();
  scan_options->fragment_scan_options =
      parquet_format->default_fragment_scan_options;
  scan_options->projection = arrow::compute::literal(true);
  scan_options->filter = arrow::compute::literal(true);
  scan_options->projected_schema = dataset->schema();
  scan_options->use_threads = true;

  arrow::dataset::ScanNodeOptions scan_node_options(dataset, scan_options);

  arrow::acero::Declaration plan{"scan", scan_node_options};
  auto start = std::chrono::high_resolution_clock::now();
  // throw_not_ok(arrow::acero::DeclarationToStatus(plan));
  auto end = std::chrono::high_resolution_clock::now();
  std::cout << "Duration: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     start)
                   .count()
            << "ms" << std::endl;

  arrow::dataset::ScanV2Options scan2(dataset);
  scan2.cache_fragment_inspection = false;
  scan2.columns = arrow::dataset::ScanV2Options::AllColumns(*dataset->schema());

  arrow::acero::Declaration plan2{"scan2", scan2};
  start = std::chrono::high_resolution_clock::now();
  throw_not_ok(arrow::acero::DeclarationToStatus(plan2));
  end = std::chrono::high_resolution_clock::now();
  std::cout << "Duration: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     start)
                   .count()
            << "ms" << std::endl;
}

} // namespace

int main() {
  try {
    RunTest();
  } catch (std::runtime_error &err) {
    std::cerr << "An error occurred: " << err.what() << std::endl;
    return 1;
  }
  return 0;
}
