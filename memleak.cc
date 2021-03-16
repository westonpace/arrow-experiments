#include <cassert>
#include <iostream>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/python/api.h>
#include <arrow/python/init.h>

void AssertOk(arrow::Status st) {
  if (!st.ok()) {
    std::cerr << "Invalid status " << st << std::endl;
    assert(false);
  }
}

int main(int argc, char const *argv[]) {

  if (argc < 2) {
    std::cerr << "You must specify a parquet file to read" << std::endl;
    return -1;
  }

  std::string pq_file = argv[1];

  auto logging_memory_pool =
      std::make_shared<arrow::LoggingMemoryPool>(arrow::system_memory_pool());
  arrow::py::set_default_memory_pool(logging_memory_pool.get());

  Py_Initialize();
  arrow_init_numpy();
  std::cout << "At start of app        : "
            << logging_memory_pool->bytes_allocated() << std::endl;
  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
  auto fs_options = arrow::dataset::FileSystemFactoryOptions();
  auto parquet_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
  auto ds_factory = *arrow::dataset::FileSystemDatasetFactory::Make(
      fs, {pq_file}, parquet_format, fs_options);
  auto ds = *ds_factory->Finish();

  auto scan_context = std::make_shared<arrow::dataset::ScanContext>();
  scan_context->pool = logging_memory_pool.get();
  auto scanner_builder = arrow::dataset::ScannerBuilder(ds, scan_context);
  auto scanner = *scanner_builder.Finish();

  auto table = *scanner->ToTable();

  std::cout << "After scanner to table : "
            << logging_memory_pool->bytes_allocated() << std::endl;

  auto convert_options = arrow::py::PandasOptions();

  PyObject *out;
  Py_BEGIN_ALLOW_THREADS;
  AssertOk(arrow::py::ConvertTableToPandas(convert_options, table, &out));
  std::cout << "After convert to pandas: "
            << logging_memory_pool->bytes_allocated() << std::endl;
  Py_END_ALLOW_THREADS;

  table.reset();
  std::cout << "After delete of table  : "
            << logging_memory_pool->bytes_allocated() << std::endl;

  Py_CLEAR(out);
  std::cout << "After Py_CLEAR         : "
            << logging_memory_pool->bytes_allocated() << std::endl;

  Py_Finalize();
};
