#pragma once

#include "writable-column.hh"

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <assert.h>
// #include <filesystem>

class DataSerializer {
public:
  DataSerializer(std::string outputFileName, unsigned long batchSize = 100000,
                 arrow::MemoryPool *pool = arrow::default_memory_pool(),
                 std::ostream *logStream = &std::cout) {
    pool_ = pool;
    out_file_name_ = outputFileName;
    log_ = logStream;
    batch_size_ = batchSize;
  }

  virtual ~DataSerializer() {
    auto status = Close();
    if (!status.ok()) {
      *(log_) << "Error occurred while closing : " << status.message()
              << std::endl;
    }
    *(log_) << "Closing the output stream to file: " << out_file_name_
            << std::endl;
    status = out_stream_->Close();
    if (!status.ok()) {
      *(log_) << "Error occurred while closing file stream : "
              << status.message() << std::endl;
    }
  }

  void SyncSizes() {
    auto n_rows_in_batch_0_ = n_rows_in_batch_;
    for (auto &col : columns_) {
      size_t len = col.second->Builder()->length();
      if (len > n_rows_in_batch_) {
        // std::cout << "Column " << col.first << " has " << len
        //           << " rows, which is more than the current batch size of "
        //           << n_rows_in_batch_ << std::endl;
        n_rows_in_batch_ = len;
      }
    }

    n_rows_total_ += (n_rows_in_batch_ - n_rows_in_batch_0_);

    for (auto &col : columns_) {
      col.second->ExtendTo(n_rows_in_batch_);
      col.second->NewRow();
    }
  }

  void NewRow() {
    // std::cout<<"New row!"<<std::endl;
    SyncSizes();

    if (n_rows_in_batch_ >= batch_size_) {
      arrow::Status writeStatus = Flush();
      if (!writeStatus.ok()) {
        *(log_) << "Error writing batch #" << n_batches_written_ << "! "
                << writeStatus.message() << std::endl;
      }
    }
  }

  arrow::Status Close() {
    SyncSizes();
    // return (n_rows_in_batch_ > 0) ? Flush() : arrow::Status::OK();
    if (n_rows_in_batch_ > 0) {
      ARROW_RETURN_NOT_OK(Flush());
    }
    return writer_->Close();
    // return arrow::Status::OK();
  }

  Column *operator[](const char *columnName) { return GetColumn(columnName); }

  Column *GetColumn(const char *columnName) {
    auto columnNode = columns_.find(columnName);
    if (columnNode == columns_.end()) {
      columns_[columnName] = std::make_unique<Column>(columnName, pool_, log_);
      if (n_rows_in_batch_ > 0)
        columns_[columnName]->ExtendTo(n_rows_in_batch_);
    }
    if (columns_[columnName]->IsInRow())
      NewRow();
    return columns_[columnName].get();
  }

  arrow::Status Flush() {
    *(log_) << "Writing batch #" << ++n_batches_written_ << " of size "
            << n_rows_in_batch_ << " to file: " << out_file_name_ << std::endl;
    if (!out_stream_) {
      ARROW_ASSIGN_OR_RAISE(out_stream_,
                            arrow::io::FileOutputStream::Open(out_file_name_));
      arrow::FieldVector fields;
      for (auto &col : columns_) {
        fields.push_back(col.second->Field());
      }
      schema_ = std::make_shared<arrow::Schema>(fields);
      ARROW_ASSIGN_OR_RAISE(
          writer_, arrow::ipc::MakeFileWriter(out_stream_.get(), schema_));
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays(columns_.size());
    for (auto &col : columns_) {
      auto iField = schema_->GetFieldIndex(col.first);
      if (iField == -1)
        return arrow::Status::KeyError("Field not found: " + col.first);

      ARROW_RETURN_NOT_OK(col.second->Builder()->Finish(&(arrays[iField])));
      if (n_rows_in_batch_ != arrays[iField]->length()) {
        std::string err_str_ =
            "Column lengths do not match! nRows = " +
            std::to_string(n_rows_in_batch_) + "whereas array" + col.first +
            "has" + std::to_string(arrays[iField]->length()) + "elements!!";
        *(log_) << err_str_ << std::endl;
        return arrow::Status::Invalid(err_str_);
      }
    }

    std::shared_ptr<arrow::RecordBatch> _rBatch =
        arrow::RecordBatch::Make(schema_, n_rows_in_batch_, arrays);
    ARROW_RETURN_NOT_OK(writer_->WriteRecordBatch(*_rBatch));

    ARROW_ASSIGN_OR_RAISE(n_bytes_written_, out_stream_->Tell());

    *(log_) << "---Wrote " << n_bytes_written_ << " bytes, @ "
            << n_bytes_written_ / n_rows_total_ << " bytes per row..."
            << std::endl;

    n_rows_in_batch_ = 0;
    return arrow::Status::OK();
  }

private:
  std::string out_file_name_;
  std::shared_ptr<arrow::io::FileOutputStream> out_stream_ = nullptr;

  std::ostream *log_ = nullptr;
  arrow::MemoryPool *pool_ = nullptr;

  unsigned long long n_rows_total_ = 0;
  unsigned long long n_bytes_written_ = 0;
  unsigned long n_rows_in_batch_ = 0;
  unsigned long n_batches_written_ = 0;
  unsigned long batch_size_ = 100000;

  std::shared_ptr<arrow::Schema> schema_ = nullptr;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer_ = nullptr;

  std::unordered_map<std::string, std::unique_ptr<Column>> columns_ = {};
};
