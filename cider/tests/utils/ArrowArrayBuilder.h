/*
 * Copyright (c) 2022 Intel Corporation.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef CIDER_ARROWARRAYBUILDER_H
#define CIDER_ARROWARRAYBUILDER_H

#include "cider/CiderBatch.h"
#include "cider/batch/CiderBatchUtils.h"
#include "exec/module/batch/ArrowABI.h"

class ArrowArrayBuilder {
 public:
  ArrowArrayBuilder()
      : row_num_(0)
      , is_row_num_set_(false)
      , allocator_(std::make_shared<CiderDefaultAllocator>()) {
    array_ = CiderBatchUtils::allocateArrowArray();
    schema_ = CiderBatchUtils::allocateArrowSchema();

    schema_->format = "+s";
    schema_->dictionary = nullptr;
    schema_->release = CiderBatchUtils::ciderEmptyArrowSchemaReleaser;
    // TODO: release stuff

    array_->buffers = (const void**)allocator_->allocate(sizeof(void*));
    array_->buffers[0] = nullptr;
    array_->n_buffers = 0;
    array_->length = 0;
    array_->offset = 0;
    array_->release = CiderBatchUtils::ciderEmptyArrowArrayReleaser;
  }

  ArrowArrayBuilder& setTableName(const std::string& table_name) {
    table_name_ = table_name;
    schema_->name = table_name.c_str();
    return *this;
  }

  ArrowArrayBuilder& setRowNum(int row_num) {
    if (is_row_num_set_) {  // have set before, throw exception
      CIDER_THROW(CiderCompileException, "row num have been set!");
    }
    is_row_num_set_ = true;
    row_num_ = row_num;
    return *this;
  }

  template <class T>
  ArrowArrayBuilder& addColumn(const std::string& col_name,
                               const ::substrait::Type& col_type,
                               const std::vector<T>& col_data,
                               const std::vector<bool>& null_data = {}) {
    if (!is_row_num_set_ ||  // have not set row num, use this col_data's row num
        row_num_ == 0) {     // previous columns are all empty
      is_row_num_set_ = true;
      row_num_ = col_data.size();
    }
    ArrowArray* current_array = new ArrowArray();
    ArrowSchema* current_schema = new ArrowSchema();

    current_schema->name = col_name.c_str();
    current_schema->format = CiderBatchUtils::convertSubstraitTypeToArrowType(col_type);
    current_schema->n_children = 0;
    current_schema->children = nullptr;
    current_schema->release = CiderBatchUtils::ciderEmptyArrowSchemaReleaser;

    if (col_data.empty()) {
      // append an empty buffer.
      array_list_.push_back(nullptr);
      schema_list_.push_back(current_schema);
      return *this;
    } else {
      // check row num
      if (row_num_ != col_data.size()) {
        CIDER_THROW(CiderCompileException, "Row num is not equal to previous columns!");
      }
      CHECK_EQ(row_num_, col_data.size());
      // check null data num
      if (!null_data.empty()) {
        CHECK_EQ(row_num_, null_data.size());
      }

      current_array->length = row_num_;
      current_array->n_children = 0;
      current_array->offset = 0;
      current_array->buffers = (const void**)allocator_->allocate(sizeof(void*) * 2);

      size_t null_size = (row_num_ + 7) >> 3;
      void* null_buf = (void*)allocator_->allocate(null_size);
      std::memset(null_buf, 0xFF, null_size);
      for (auto i = 0; i < null_data.size(); i++) {
        if (null_data[i]) {
          CiderBitUtils::clearBitAt((uint8_t*)null_buf, i);
        }
      }
      // TODO: null_count

      current_array->buffers[0] = null_buf;
      current_array->buffers[1] =
          (void*)allocator_->allocate(sizeof(T) * col_data.size());
      memcpy(const_cast<void*>(current_array->buffers[1]),
             col_data.data(),
             sizeof(T) * col_data.size());
      current_array->n_buffers = 2;
      current_array->private_data = nullptr;
      current_array->dictionary = nullptr;
      current_array->release = CiderBatchUtils::ciderEmptyArrowArrayReleaser;

      array_list_.push_back(current_array);
      schema_list_.push_back(current_schema);
    }
    return *this;
  }

  // TODO: bool string varchar date

  std::tuple<ArrowSchema*&, ArrowArray*&> build() {
    if (!is_row_num_set_) {
      CIDER_THROW(CiderCompileException, "Invalid build!");
    }
    array_->length = row_num_;
    // TODO: null_count
    size_t column_num = array_list_.size();
    array_->n_children = column_num;
    array_->children =
        (ArrowArray**)allocator_->allocate(sizeof(ArrowArray*) * column_num);
    memcpy(array_->children, array_list_.data(), sizeof(ArrowArray*) * column_num);

    schema_->n_children = schema_list_.size();
    schema_->children =
        (ArrowSchema**)allocator_->allocate(sizeof(ArrowSchema*) * column_num);
    memcpy(schema_->children, schema_list_.data(), sizeof(ArrowSchema*) * column_num);

    return {schema_, array_};
  }

 private:
  std::string table_name_ = "";
  size_t row_num_;
  bool is_row_num_set_;

  std::vector<ArrowArray*> array_list_;
  std::vector<ArrowSchema*> schema_list_;

  ArrowSchema* schema_;
  ArrowArray* array_;

  std::shared_ptr<CiderAllocator> allocator_;
};

#endif  // CIDER_ARROWARRAYBUILDER_H
