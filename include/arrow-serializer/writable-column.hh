#pragma once

#include <arrow/api.h>
#include <arrow/result.h>

#include <string>
#include <vector>
#include <iostream>

class Column{
public:
    Column(std::string colname, arrow::MemoryPool* pool = arrow::default_memory_pool(), std::ostream* logStream = &std::cout){
        pool_ = pool;
        column_name_ = colname;
    }

    virtual ~Column(){}

    bool IsInitialized(){return is_initialized_;}

    bool IsInRow(){return is_in_row_;}

    void NewRow(){is_in_row_ = false;}
    
    std::shared_ptr<arrow::Field> Field(){return field_;}

    void Append(){
        arrow::Status status = builder_->AppendNull();
        if(!(status.ok())){
            *(log_)<<"Error appending null to column "<<column_name_<<": "<<status.message()<<std::endl;
        }
        is_in_row_ = true;
    }

    void Extend(size_t n){
        if(n==1){
            Append(); 
            return;
        }   
        arrow::Status status = builder_->AppendNulls(n);
        if(!(status.ok())){
            *(log_)<<"Error appending nulls to column "<<column_name_<<": "<<status.message()<<std::endl;
        }
        is_in_row_ = true;
    }

    void ExtendTo(size_t n){
        if(n <= builder_->length())return;
        Extend(n - builder_->length());
    }

    template<typename T> void Append(const T& value){
        arrow::Status status = AppendImpl<T>(Builder<T>(), value);
        if(!(status.ok())){
            *(log_)<<"Error appending to column "<<column_name_<<": "<<status.message()<<std::endl;
        }
        is_in_row_ = true;
    }

    template<typename T> void Extend(const std::vector<T>& values){
        arrow::Status status = ExtendImpl<T>(Builder<T>(), values);
        if(!(status.ok())){
            *(log_)<<"Error extending column "<<column_name_<<": "<<status.message()<<std::endl;
        }
        is_in_row_ = true;
    }

    arrow::ArrayBuilder* Builder() {
        if(!is_initialized_)return nullptr;
        else return builder_.get();
    }

    template <
        typename T,
        typename Builder_t = typename arrow::CTypeTraits<T>::BuilderType
    > Builder_t* Builder() {
        if(!is_initialized_){
            Initialize<T>();
        }
        return static_cast<Builder_t*>(builder_.get());
    }

private:
    template <typename T> void Initialize() {
        builder_ = MakeBuilder<T>();
        field_ = arrow::field(column_name_, builder_->type());
        is_initialized_ = true;
    }

    template <
        typename T, 
        typename Arrow_t = typename arrow::CTypeTraits<T>::ArrowType, 
        typename Builder_t = typename arrow::CTypeTraits<T>::BuilderType
    > arrow::enable_if_primitive_ctype<Arrow_t, std::shared_ptr<Builder_t>> MakeBuilder() {
        return std::make_shared<Builder_t>(pool_);
    }

    template <
        typename T, 
        typename Arrow_t = typename arrow::CTypeTraits<T>::ArrowType, 
        typename Builder_t = typename arrow::CTypeTraits<T>::BuilderType
    > arrow::enable_if_primitive_ctype<Arrow_t, arrow::Status> AppendImpl(Builder_t* builder, const T& value) {
        return builder->Append(value);
    }

    template <
        typename T, 
        typename Arrow_t = typename arrow::CTypeTraits<T>::ArrowType, 
        typename Builder_t = typename arrow::CTypeTraits<T>::BuilderType
    > arrow::enable_if_primitive_ctype<Arrow_t, arrow::Status> ExtendImpl(Builder_t* builder, const std::vector<T>& values) {
        return builder->AppendValues(values);
    }

    template <
        typename T, 
        typename Arrow_t = typename arrow::CTypeTraits<T>::ArrowType, 
        typename Builder_t = typename arrow::CTypeTraits<T>::BuilderType
    > arrow::enable_if_list_type<Arrow_t, std::shared_ptr<Builder_t>> MakeBuilder() {
        return std::make_shared<Builder_t>(pool_, MakeBuilder<typename T::value_type>());
    }

    template <
        typename T, 
        typename Arrow_t = typename arrow::CTypeTraits<T>::ArrowType, 
        typename Builder_t = typename arrow::CTypeTraits<T>::BuilderType
    > arrow::enable_if_list_type<Arrow_t, arrow::Status> AppendImpl(Builder_t* builder, const T& value) {
        using Value_t = typename T::value_type;
        using ValueBuilder_t = typename arrow::CTypeTraits<Value_t>::BuilderType;
        ARROW_RETURN_NOT_OK(builder->Append());
        return ExtendImpl<Value_t>(static_cast<ValueBuilder_t*>(builder->value_builder()), value);        
    }

    template <
        typename T, 
        typename Arrow_t = typename arrow::CTypeTraits<T>::ArrowType, 
        typename Builder_t = typename arrow::CTypeTraits<T>::BuilderType
    > arrow::enable_if_list_type<Arrow_t, arrow::Status> ExtendImpl(Builder_t* builder, const std::vector<T>& values) {
        for(const T& value : values){
            ARROW_RETURN_NOT_OK(AppendImpl<T>(builder, value));
        }
        return arrow::Status::OK();
    }

    arrow::MemoryPool* pool_ = nullptr;
    std::ostream* log_ = nullptr;

    bool is_initialized_ = false;
    bool is_in_row_ = false;

    std::string column_name_;
    std::shared_ptr<arrow::ArrayBuilder> builder_;
    std::shared_ptr<arrow::Field> field_;
};
    