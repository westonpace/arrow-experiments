#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <unistd.h>
#include <iostream>
#include <jemalloc/jemalloc.h>

std::shared_ptr<arrow::Table> generate_table() {
  arrow::Int64Builder i64builder;
  for (int i=0;i<5*320000;i++){
    i64builder.Append(i);
  }
  std::shared_ptr<arrow::Array> i64array;
  PARQUET_THROW_NOT_OK(i64builder.Finish(&i64array));

  std::shared_ptr<arrow::Schema> schema = arrow::schema(
      {arrow::field("int", arrow::int64())});

  return arrow::Table::Make(schema, {i64array});
}

void write_parquet_file(const arrow::Table& table) {
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  PARQUET_ASSIGN_OR_THROW(
      outfile, arrow::io::FileOutputStream::Open("parquet-arrow-example.parquet"));
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 100000));
}

void read_whole_file() {
  std::cout << "Reading parquet-arrow-example.parquet at once" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(infile,
                          arrow::io::ReadableFile::Open("parquet-arrow-example.parquet",
                                                        arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
  std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
            << " columns." << std::endl;
}

int main(int argc, char** argv) {
  std::cout << "running" << std::endl;
  std::shared_ptr<arrow::Table> table = generate_table();
  write_parquet_file(*table);
  std::cout << "start " <<std::endl;
  read_whole_file();
  std::cout << "end " <<std::endl;
  table.reset();
  arrow::default_memory_pool()->ReleaseUnused();
  std::cout << arrow::default_memory_pool()->backend_name() << " " << arrow::default_memory_pool()->bytes_allocated() << " bytes_allocated" << std::endl;
  sleep(100);
  malloc_stats_print(NULL, NULL, NULL);
  std::cout << "ARRROOOOOWWWWW" << std::endl;
  arrow::default_memory_pool()->PrintStats();
}
