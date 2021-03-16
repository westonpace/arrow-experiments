
// Client side C/C++ program to demonstrate Socket programming
#include <arpa/inet.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

constexpr uint16_t port = 56565;
constexpr char host[] = "127.0.0.1";

bool CheckErr(int err, std::string activity) {
  if (err < 0) {
    std::cerr << "Received error code " << err << " while calling " << activity
              << std::endl;
    return false;
  }
  return true;
}

bool CheckErr(arrow::Status status, std::string activity) {
  if (!status.ok()) {
    std::cerr << "Recevied err status " << status << " while calling "
              << activity << std::endl;
    return false;
  }
  return true;
}

class SocketOutputStream : public arrow::io::OutputStream {

public:
  SocketOutputStream(std::shared_ptr<arrow::io::FileOutputStream> target)
      : target_(target), position_(0) {}

  virtual ~SocketOutputStream() {}

  arrow::Status Close() override { return target_->Close(); }
  arrow::Status Abort() override { return target_->Abort(); }
  bool closed() const override { return target_->closed(); }
  arrow::Status Flush() override { return target_->Flush(); }

  static arrow::Result<std::shared_ptr<SocketOutputStream>> Open(int sock) {
    auto target_res = arrow::io::FileOutputStream::Open(sock);
    if (!target_res.ok()) {
      return target_res.status();
    }
    return std::make_shared<SocketOutputStream>(*target_res);
  }

  arrow::Status Write(const void *data, int64_t nbytes) override {
    position_ += nbytes;
    return target_->Write(data, nbytes);
  }

  arrow::Status Write(const std::shared_ptr<arrow::Buffer> &data) override {
    position_ += data->size();
    return target_->Write(data);
  }

  arrow::Result<int64_t> Tell() const override { return position_; }

private:
  std::shared_ptr<arrow::io::FileOutputStream> target_;
  uint64_t position_;
};

std::shared_ptr<arrow::Table> MakeTable() {
  arrow::MemoryPool *pool = arrow::default_memory_pool();
  arrow::Int64Builder values_builder(pool);
  values_builder.Append(1);
  values_builder.Append(2);
  values_builder.Append(3);
  std::shared_ptr<arrow::Int64Array> arr;
  if (!CheckErr(values_builder.Finish(&arr), "values_builder::Finish")) {
    return nullptr;
  }

  std::vector<std::shared_ptr<arrow::Field>> fields = {
      arrow::field("values", arrow::int64())};
  auto schema = std::make_shared<arrow::Schema>(fields);
  return arrow::Table::Make(schema, {arr});
}

void SendTable(int socket_fd) {
  auto output_res = SocketOutputStream::Open(socket_fd);
  if (!CheckErr(output_res.status(), "arrow::io::FileOutputStream")) {
    return;
  }
  auto output = *output_res;

  arrow::MemoryPool *pool = arrow::default_memory_pool();

  auto table = MakeTable();
  if (table == nullptr) {
    return;
  }

  auto writer_res = arrow::ipc::MakeStreamWriter(output, table->schema());
  if (!CheckErr(writer_res.status(), "arrow::ipc::MakeStreamWriter")) {
    return;
  }
  auto writer = *writer_res;
  if (!CheckErr(writer->WriteTable(*table), "RecordBatchWriter::WriteTable")) {
    return;
  }
  CheckErr(writer->Close(), "RecordBatchWriter::Close");
}

int main(int argc, char const *argv[]) {
  struct sockaddr_in addr;
  char hello[] = "Hello from client";
  char buffer[1024] = {0};
  int sock = socket(AF_INET, SOCK_STREAM, 0);

  if (!CheckErr(sock, "socket")) {
    return -1;
  }

  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);

  // Convert IPv4 and IPv6 addresses from text to binary form
  if (!CheckErr(inet_pton(AF_INET, host, &addr.sin_addr), "inet_pton")) {
    return -2;
  }

  if (!CheckErr(connect(sock, (struct sockaddr *)&addr, sizeof(addr)),
                "connect")) {
    return -3;
  }

  SendTable(sock);
  printf("Table sent\n");
  return 0;
}
