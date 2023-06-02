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

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/io/compressed.h>
#include <arrow/ipc/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/iterator.h>
#include <arrow/util/logging.h>

using namespace std::chrono;

namespace arrow {

  Result<std::shared_ptr<dataset::Dataset>> MakeDataset(const std::string& filename) {
    auto fs = std::make_shared<fs::LocalFileSystem>();
    auto fs_dataset_options = arrow::dataset::FileSystemFactoryOptions();
    std::shared_ptr<arrow::dataset::FileFormat> format;
    if (filename.find("feather") == std::string::npos) {
      format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    } else {
      format = std::make_shared<arrow::dataset::IpcFileFormat>();
    }
    ARROW_ASSIGN_OR_RAISE(auto dataset_factory,
                          arrow::dataset::FileSystemDatasetFactory::Make(
                              fs, {filename}, format, fs_dataset_options));
    auto finish_options = arrow::dataset::FinishOptions();
    finish_options.validate_fragments = false;

    auto inspect_options = arrow::dataset::InspectOptions();
    inspect_options.fragments = 1;
    finish_options.inspect_options = inspect_options;
    return dataset_factory->Finish(finish_options);
  }

  Result<std::shared_ptr<dataset::ScanOptions>> DefaultOptions(std::shared_ptr<dataset::Dataset> dataset, uint64_t batch_size) {
    auto scanner_builder = std::make_shared<dataset::ScannerBuilder>(std::move(dataset));
    ARROW_RETURN_NOT_OK(scanner_builder->UseThreads(true));
    ARROW_RETURN_NOT_OK(scanner_builder->BatchSize(batch_size));
    ARROW_RETURN_NOT_OK(scanner_builder->Project({"l_discount", "l_quantity", "l_shipdate", "l_extendedprice"}));
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
    return scanner->options();
  }

  Status RunMain(int argc, char** argv) {
    std::string filename = argv[1];
    int capacity = atoi(argv[2]);
    int batch_size = atoi(argv[3]);
    std::cout << "Using capacity " << capacity << std::endl;
    std::cout << "Using bch size " << batch_size << std::endl;
    ARROW_RETURN_NOT_OK(internal::GetCpuThreadPool()->SetCapacity(capacity));
    dataset::internal::Initialize();
    ARROW_ASSIGN_OR_RAISE(auto line_items_dataset, MakeDataset(filename));
    ARROW_ASSIGN_OR_RAISE(auto line_items_scan_opts, DefaultOptions(line_items_dataset, batch_size));

    auto start_date = std::make_shared<arrow::Date32Scalar>(8766);
    auto end_date = std::make_shared<arrow::Date32Scalar>(9131);
    auto min_discount = std::make_shared<arrow::DoubleScalar>(0.05);
    auto max_discount = std::make_shared<arrow::DoubleScalar>(0.07);
    auto max_quantity = std::make_shared<arrow::DoubleScalar>(32);

    auto cast_to_double_opts = arrow::compute::CastOptions();
    cast_to_double_opts.to_type = arrow::float64();

    auto ship_after_start = arrow::compute::greater_equal(arrow::compute::field_ref("l_shipdate"), arrow::compute::literal(start_date));
    auto ship_before_end = arrow::compute::less_equal(arrow::compute::field_ref("l_shipdate"), arrow::compute::literal(end_date));
    auto ship_in_range = arrow::compute::and_(ship_after_start, ship_before_end);
    auto cast_discount_1 = arrow::compute::call("cast", {arrow::compute::field_ref("l_discount")}, cast_to_double_opts);
    auto discount_after_min = arrow::compute::greater_equal(cast_discount_1, arrow::compute::literal(min_discount));
    auto anded_two = arrow::compute::and_(ship_in_range, discount_after_min);
    auto cast_discount_2 = arrow::compute::call("cast", {arrow::compute::field_ref("l_discount")}, cast_to_double_opts);
    auto discount_before_max = arrow::compute::less_equal(cast_discount_2, arrow::compute::literal(max_discount));
    auto anded_three = arrow::compute::and_(anded_two, discount_before_max);
    auto cast_quantity = arrow::compute::call("cast", {arrow::compute::field_ref("l_quantity")}, cast_to_double_opts);
    auto quantity_before_max = arrow::compute::less_equal(cast_quantity, arrow::compute::literal(max_quantity));
    auto expression = arrow::compute::and_(anded_three, quantity_before_max);

    auto exec_context = compute::ExecContext(default_memory_pool(), internal::GetCpuThreadPool());
    ARROW_ASSIGN_OR_RAISE(auto plan, compute::ExecPlan::Make(&exec_context));
    ARROW_ASSIGN_OR_RAISE(auto line_items_scan, compute::MakeExecNode("scan", plan.get(), {}, dataset::ScanNodeOptions{line_items_dataset, line_items_scan_opts}));
    ARROW_ASSIGN_OR_RAISE(auto filtered_line_items, compute::MakeExecNode("filter", plan.get(), {line_items_scan}, compute::FilterNodeOptions{expression}));

    auto revenue_expr = arrow::compute::call("multiply_checked", {arrow::compute::field_ref("l_extendedprice"), arrow::compute::field_ref("l_discount")});
    ARROW_ASSIGN_OR_RAISE(auto projected_revenue, compute::MakeExecNode("project", plan.get(), {filtered_line_items}, compute::ProjectNodeOptions{{revenue_expr}, {"revenue"}}));

    auto hash_sum_opts = compute::ScalarAggregateOptions(true, 1);
    auto aggregate = compute::internal::Aggregate{"sum", &hash_sum_opts};
    ARROW_ASSIGN_OR_RAISE(
        auto grouped,
        compute::MakeExecNode(
            "aggregate", plan.get(), {projected_revenue},
            compute::AggregateNodeOptions{{aggregate},
                                          {{"revenue"}},
                                          {"revenue"},
                                          {}}));

    std::function<Future<util::optional<compute::ExecBatch>>()> sink_gen;
    ARROW_RETURN_NOT_OK(compute::MakeExecNode("sink", plan.get(), {grouped}, compute::SinkNodeOptions{&sink_gen}));

    std::cout << plan->ToString() << std::endl;
    auto start = high_resolution_clock::now();
    ARROW_RETURN_NOT_OK(plan->StartProducing());
    Status final_st = plan->finished().status();
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<nanoseconds>(end - start).count();
    std::cout << duration << std::endl;
    return final_st;
  }

}

int main(int argc, char **argv) {
  arrow::Status st = arrow::RunMain(argc, argv);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
