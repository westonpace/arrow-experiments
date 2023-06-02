#include <iostream>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/util/async_generator.h>

arrow::Result<std::shared_ptr<arrow::Table>> ReadSampleCsvFile(std::string filename) {
  std::shared_ptr<arrow::fs::FileSystem> fs = std::make_shared<arrow::fs::LocalFileSystem>();
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::io::InputStream> input, fs->OpenInputStream(filename));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::csv::TableReader> reader,
                        arrow::csv::TableReader::Make(arrow::io::default_io_context(),
                                                      input,
                                                      arrow::csv::ReadOptions(),
                                                      arrow::csv::ParseOptions(),
                                                      arrow::csv::ConvertOptions()));
  return reader->Read();
}

arrow::Result<arrow::compute::ExecNode*> TableToPlanSource(std::shared_ptr<arrow::Table> table, arrow::compute::ExecPlan* plan) {
  std::shared_ptr<arrow::dataset::Dataset> dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);
  // We are using a scanner builder to get scan options which means we create a scanner and then throw
  // the scanner away.  It's a bit clumsy and will probably get simplified beyond 7.0.0
  arrow::dataset::ScannerBuilder scanner_builder(dataset);
  // This next line will go away in 7.0.0
  ARROW_RETURN_NOT_OK(scanner_builder.UseAsync(true));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Scanner> scanner, scanner_builder.Finish());
  std::shared_ptr<arrow::dataset::ScanOptions> scan_options = scanner->options();
  arrow::dataset::ScanNodeOptions scan_node_options(dataset, scan_options);
  return arrow::compute::MakeExecNode("scan", plan, {}, scan_node_options);
}

arrow::Result<std::shared_ptr<arrow::Table>> PlanToTable(arrow::compute::ExecNode* terminal_node, arrow::compute::ExecPlan* plan) {
  std::function<arrow::Future<arrow::util::optional<arrow::compute::ExecBatch>>()> sink_gen;
  ARROW_RETURN_NOT_OK(arrow::compute::MakeExecNode(
      "sink", plan, {terminal_node}, arrow::compute::SinkNodeOptions{&sink_gen}));
  std::shared_ptr<arrow::Schema> final_schema = terminal_node->output_schema();
  std::function<arrow::Future<std::shared_ptr<arrow::RecordBatch>>()> batch_gen =
      arrow::MakeMappedGenerator(sink_gen, [final_schema] (const arrow::util::optional<arrow::compute::ExecBatch>& batch) -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
            if (arrow::IsIterationEnd(batch)) {
              return arrow::IterationEnd<std::shared_ptr<arrow::RecordBatch>>();
            }
            return batch->ToRecordBatch(final_schema);
          });
  ARROW_RETURN_NOT_OK(plan->StartProducing());
  arrow::Future<std::vector<std::shared_ptr<arrow::RecordBatch>>> batches_fut = arrow::CollectAsyncGenerator(batch_gen);
  ARROW_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<arrow::RecordBatch>> batches, batches_fut.result());
  ARROW_RETURN_NOT_OK(plan->finished().status());
  return arrow::Table::FromRecordBatches(batches);
}

arrow::Result<std::shared_ptr<arrow::Table>> GroupByOrigin(std::shared_ptr<arrow::Table> input) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::compute::ExecPlan> plan, arrow::compute::ExecPlan::Make());
  ARROW_ASSIGN_OR_RAISE(arrow::compute::ExecNode* source, TableToPlanSource(std::move(input), plan.get()));
  ARROW_ASSIGN_OR_RAISE(arrow::compute::ExecNode* group_by,
                        arrow::compute::MakeExecNode(
                            "aggregate",
                            plan.get(),
                            {source},
                            arrow::compute::AggregateNodeOptions(
                                {{"hash_mean", /*Default options*/ nullptr}},
                                {"Horsepower"},
                                {"Avg Horsepower"},
                                {"Origin"})));
  return PlanToTable(group_by, plan.get());
}

void PrintTable(std::shared_ptr<arrow::Table> table) {
  std::shared_ptr<arrow::Array> avg_hp_generic = table->column(0)->chunk(0);
  std::shared_ptr<arrow::DoubleArray> avg_hp = std::dynamic_pointer_cast<arrow::DoubleArray>(avg_hp_generic);
  std::shared_ptr<arrow::Array> origin_generic = table->column(1)->chunk(0);
  std::shared_ptr<arrow::StringArray> origin = std::dynamic_pointer_cast<arrow::StringArray>(origin_generic);
  std::cout << "Average Horsepower,Origin" << std::endl;
  for(int i = 0; i < table->num_rows(); i++) {
    std::cout << avg_hp->Value(i) << "," << origin->Value(i) << std::endl;
  }
}

arrow::Status RunMain(int argc, char **argv) {
  if (argc < 2) {
    return arrow::Status::Invalid("Usage: group_by_example <csv-filename>");
  }
  // This (and the required import of dataset/plan.h) will be going away someday
  arrow::dataset::internal::Initialize();
  std::string filename = argv[1];
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> sample_table, ReadSampleCsvFile(filename));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> grouped_table, GroupByOrigin(sample_table));
  PrintTable(grouped_table);
  return arrow::Status::OK();
}

int main(int argc, char **argv) {
  arrow::Status st = RunMain(argc, argv);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
