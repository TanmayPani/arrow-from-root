#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <fstream>
#include <string>
#include <vector>
#include <iostream>
#include <assert.h>
#include <filesystem> 
#include <cstdlib>

class Column {
public:
    Column(std::string colname, arrow::Table* table, std::ostream* logStream = &std::cout) {
        column_name_ = colname;
        log_ = logStream;
        column_ = table->GetColumnByName(column_name_);
    }

    virtual ~Column() {}

    
        
private:
    std::ostream* log_;
    std::string column_name_;
    std::shared_ptr<arrow::ChunkedArray> column_;
};

arrow::Status readFile(const std::string& inFileStr, arrow::MemoryPool* pool = arrow::default_memory_pool())    {
    std::cout<<"Reading file: "<<inFileStr<<std::endl;

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::io::MemoryMappedFile> inFileStream, 
                            arrow::io::MemoryMappedFile::Open(inFileStr, arrow::io::FileMode::READ));
    
    arrow::ipc::IpcReadOptions ipcReadOpt;
    ipcReadOpt.memory_pool = pool;
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipcRbReader, 
                            arrow::ipc::RecordBatchFileReader::Open(inFileStream.get(), ipcReadOpt));

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    auto nbatches = ipcRbReader->num_record_batches();
    batches.resize(nbatches);
    for(auto ibatch = 0; ibatch < nbatches; ++ibatch){
        ARROW_ASSIGN_OR_RAISE(batches[ibatch], ipcRbReader->ReadRecordBatch(ibatch));
    }

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, arrow::Table::FromRecordBatches(std::move(batches)));
    std::cout<<"nrows: "<<table->num_rows()<<std::endl;

    std::shared_ptr<arrow::ChunkedArray> genJetPtArr = table->GetColumnByName("genJet_pt");
    auto nJets = genJetPtArr->length();
    std::cout<<"Total number of jets: "<<nJets<<std::endl;


    for(auto iJet = 0; iJet < nJets; iJet++){
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Scalar> ptScalar, genJetPtArr->GetScalar(iJet));
        double pt = static_cast<arrow::DoubleScalar*>(ptScalar.get())->value;
    }


    //std::cout<<"ncols: "<<table->num_columns()<<std::endl;
    //std::cout<<"nrows: "<<table->num_rows()<<std::endl;
    //std::cout<<"Schema: "<<std::endl;
    //std::cout<<table->schema()->ToString()<<std::endl;

    
    //std::cout<<"Has "<<ipcRbReader->num_record_batches()<<" record batches"<<std::endl;
    //ARROW_ASSIGN_OR_RAISE(unsigned nrows, ipcRbReader->CountRows());
    //std::cout<<"Has "<<nrows<<" rows"<<std::endl;
    //std::cout<<"Schema: "<<std::endl;
    //std::cout<<ipcRbReader->schema()->ToString()<<std::endl;

    return arrow::Status::OK();

}