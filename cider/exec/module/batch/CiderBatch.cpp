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

#include "cider/batch/CiderBatch.h"
#include "ArrowABI.h"

CiderBatch::CiderBatch(ArrowSchema* schema, std::shared_ptr<CiderAllocator> allocator)
    : arrow_schema_(schema), ownership_(true), reallocate_(true), allocator_(allocator) {
  CHECK(arrow_schema_);
  CHECK(arrow_schema_->release);
  arrow_array_ = CiderBatchUtils::allocateArrowArray();
  arrow_array_->n_buffers = CiderBatchUtils::getBufferNum(arrow_schema_);
  arrow_array_->n_children = arrow_schema_->n_children;
  CiderArrowArrayBufferHolder* root_holder =
      new CiderArrowArrayBufferHolder(arrow_array_->n_buffers,
                                      arrow_schema_->n_children,
                                      allocator_,
                                      arrow_schema_->dictionary);
  arrow_array_->buffers = root_holder->getBufferPtrs();
  arrow_array_->children = root_holder->getChildrenPtrs();
  arrow_array_->dictionary = root_holder->getDictPtr();
  arrow_array_->private_data = root_holder;
  arrow_array_->release = CiderBatchUtils::ciderArrowArrayReleaser;
}

CiderBatch::CiderBatch(ArrowSchema* schema,
                       ArrowArray* array,
                       std::shared_ptr<CiderAllocator> allocator)
    : arrow_schema_(schema)
    , arrow_array_(array)
    , ownership_(true)
    , reallocate_(false)
    , allocator_(allocator) {
  CHECK(arrow_schema_);
  CHECK(arrow_schema_->release);
  CHECK(arrow_array_);
  CHECK(arrow_array_->release);
  CHECK(allocator_);
}

CiderBatch::~CiderBatch() {
  releaseArrowEntries();
  destroy();  // TODO: Remove
}

CiderBatch::CiderBatch(const CiderBatch& rh) {
  this->arrow_array_ = rh.arrow_array_;
  this->arrow_schema_ = rh.arrow_schema_;
  this->ownership_ = false;
  this->reallocate_ = rh.reallocate_;
  this->allocator_ = rh.allocator_;
}

CiderBatch& CiderBatch::operator=(const CiderBatch& rh) {
  if (&rh == this) {
    return *this;
  }
  releaseArrowEntries();

  this->arrow_array_ = rh.arrow_array_;
  this->arrow_schema_ = rh.arrow_schema_;
  this->ownership_ = false;
  this->reallocate_ = rh.reallocate_;
  this->allocator_ = rh.allocator_;

  return *this;
}

CiderBatch::CiderBatch(CiderBatch&& rh) noexcept {
  this->arrow_array_ = rh.arrow_array_;
  this->arrow_schema_ = rh.arrow_schema_;
  this->ownership_ = rh.ownership_;
  this->reallocate_ = rh.reallocate_;
  this->allocator_ = rh.allocator_;

  rh.arrow_array_ = nullptr;
  rh.arrow_schema_ = nullptr;
  rh.ownership_ = false;
  rh.reallocate_ = false;

  moveFrom(&rh);  // TODO: Remove
}

CiderBatch& CiderBatch::operator=(CiderBatch&& rh) noexcept {
  if (this == &rh) {
    return *this;
  }
  releaseArrowEntries();
  this->arrow_array_ = rh.arrow_array_;
  this->arrow_schema_ = rh.arrow_schema_;
  this->ownership_ = rh.ownership_;
  this->reallocate_ = rh.reallocate_;
  this->allocator_ = rh.allocator_;

  rh.arrow_array_ = nullptr;
  rh.arrow_schema_ = nullptr;
  rh.ownership_ = false;
  rh.reallocate_ = false;

  moveFrom(&rh);  // TODO: Remove

  return *this;
}

size_t CiderBatch::getBufferNum() const {
  return arrow_array_->n_buffers;
}

size_t CiderBatch::getChildrenNum() const {
  return arrow_schema_->n_children;
}

SQLTypes CiderBatch::getCiderType() const {
  return CiderBatchUtils::convertArrowTypeToCiderType(arrow_schema_->format);
}

// TODO: Dictionary support is TBD.
std::unique_ptr<CiderBatch> CiderBatch::getChildAt(size_t index) {
  CHECK(!isMoved());
  CHECK_LT(index, arrow_schema_->n_children);
  ArrowSchema* child_schema = arrow_schema_->children[index];
  ArrowArray* child_array = arrow_array_->children[index];

  if (!child_schema || !child_schema->release) {
    // Child has been moved.
    return nullptr;
  }

  if (child_array->release == nullptr) {
    // Lazy allocate child array.
    child_array->n_buffers = CiderBatchUtils::getBufferNum(child_schema);
    child_array->n_children = child_schema->n_children;
    CiderArrowArrayBufferHolder* holder =
        new CiderArrowArrayBufferHolder(child_array->n_buffers,
                                        child_schema->n_children,
                                        allocator_,
                                        child_schema->dictionary);
    child_array->buffers = holder->getBufferPtrs();
    child_array->children = holder->getChildrenPtrs();
    child_array->dictionary = holder->getDictPtr();
    child_array->private_data = holder;
    child_array->release = CiderBatchUtils::ciderArrowArrayReleaser;
  }

  auto child_batch =
      CiderBatchUtils::createCiderBatch(allocator_, child_schema, child_array);
  child_batch->ownership_ = false;  // Only root batch has ownership.
  child_batch->reallocate_ =
      true;  // ArrowArray allocated from Cider could (re-)allocate buffer.

  return child_batch;
}

bool CiderBatch::resizeBatch(int64_t size, bool default_not_null) {
  if (getNulls()) {
    if (!resizeNulls(size, default_not_null)) {
      return false;
    }
  }
  if (!resizeData(size)) {
    return false;
  }
  return true;
}

bool CiderBatch::resizeNulls(int64_t size, bool default_not_null) {
  CHECK(!isMoved());
  if (!permitBufferAllocate()) {
    return false;
  }

  ArrowArray* array = getArrowArray();
  auto array_holder = reinterpret_cast<CiderArrowArrayBufferHolder*>(array->private_data);

  size_t bytes = ((size + 7) >> 3);
  size_t null_index = getNullVectorIndex();
  bool first_time = !array->buffers[null_index];

  array_holder->allocBuffer(null_index, bytes);
  uint8_t* null_vector = array_holder->getBufferAs<uint8_t>(null_index);

  if (default_not_null) {
    // TODO: Optimize
    for (size_t i = (first_time ? 0 : getLength()); i < size; ++i) {
      CiderBitUtils::setBitAt(null_vector, i);
    }
  } else {
    // TODO: Optimize
    for (size_t i = (first_time ? 0 : getLength()); i < size; ++i) {
      CiderBitUtils::clearBitAt(null_vector, i);
    }
  }
  size_t not_null_num = CiderBitUtils::countSetBits(null_vector, size);
  setNullCount(size - not_null_num);

  return true;
}

uint8_t* CiderBatch::getMutableNulls() {
  CHECK(!isMoved());
  ArrowArray* array = getArrowArray();
  const void* nulls = array->buffers[getNullVectorIndex()];
  if (!nulls) {
    if (resizeNulls(getLength(), true)) {
      return reinterpret_cast<uint8_t*>(
          const_cast<void*>(array->buffers[getNullVectorIndex()]));
    }
  }
  return nullptr;
}

const uint8_t* CiderBatch::getNulls() const {
  CHECK(!isMoved());
  ArrowArray* array = getArrowArray();

  return reinterpret_cast<const uint8_t*>(array->buffers[0]);
}

void CiderBatch::releaseArrowEntries() {
  if (ownership_) {
    if (arrow_schema_) {
      if (arrow_schema_->release) {
        arrow_schema_->release(arrow_schema_);
      }
      CiderBatchUtils::freeArrowSchema(arrow_schema_);
      arrow_schema_ = nullptr;
    }
    if (arrow_array_) {
      if (arrow_array_->release) {
        arrow_array_->release(arrow_array_);
      }
      CiderBatchUtils::freeArrowArray(arrow_array_);
      arrow_array_ = nullptr;
    }
  }
}

void CiderBatch::move(ArrowSchema& schema, ArrowArray& array) {
  CHECK(!isMoved());

  schema = *arrow_schema_;
  array = *arrow_array_;
  arrow_schema_->release = nullptr;
  arrow_array_->release = nullptr;
}

std::pair<ArrowSchema*, ArrowArray*> CiderBatch::move() {
  CHECK(!isMoved());
  ArrowSchema* schema = CiderBatchUtils::allocateArrowSchema();
  ArrowArray* array = CiderBatchUtils::allocateArrowArray();

  *schema = *arrow_schema_;
  *array = *arrow_array_;

  arrow_schema_->release = nullptr;
  arrow_array_->release = nullptr;

  return {schema, array};
}

CiderBatch::SchemaReleaser CiderBatch::getSchemaReleaser() const {
  return arrow_schema_->release;
}

CiderBatch::ArrayReleaser CiderBatch::getArrayReleaser() const {
  return arrow_array_->release;
}

void* CiderBatch::getSchemaPrivate() const {
  return arrow_schema_->private_data;
}

void* CiderBatch::getArrayPrivate() const {
  return arrow_array_->private_data;
}

const void** CiderBatch::getBuffersPtr() const {
  CHECK(!isMoved());
  return arrow_array_->buffers;
}

const void** CiderBatch::getChildrenArrayPtr() const {
  CHECK(!isMoved());
  return const_cast<const void**>(reinterpret_cast<void**>(arrow_array_->children));
}

void CiderBatch::setNullCount(int64_t null_num) {
  CHECK(!isMoved());
  arrow_array_->null_count = null_num;
}

int64_t CiderBatch::getNullCount() const {
  CHECK(!isMoved());
  return arrow_array_->null_count;
}

void CiderBatch::setLength(int64_t length) {
  CHECK(!isMoved());
  arrow_array_->length = length;
}

int64_t CiderBatch::getLength() const {
  CHECK(!isMoved());
  return arrow_array_->length;
}

bool CiderBatch::isMoved() const {
  CHECK((arrow_array_ && arrow_schema_) || (!arrow_array_ && !arrow_schema_));
  if (arrow_schema_ && arrow_array_) {
    CHECK((arrow_schema_->release && arrow_array_->release) ||
          (!arrow_schema_->release && !arrow_array_->release));
    return !arrow_array_->release;
  }
  return true;
}

bool CiderBatch::containsNull() const {
  CHECK(!isMoved());
  return getNulls() && getNullCount();
}

void CiderBatch::sort(const SortInfo& sort_info) {
  if (is_sorted_ || sort_info.order_entries.size() == 0) {
    return;
  }
  // get result table
  int row_num = row_num_;
  int col_num = column_num();
  auto types = schema_->getColumnTypes();
  std::vector<std::vector<int8_t*>> table_ptr_vec = getRowTable();
  // sort result table
  generator::ResultSetComparator rsc = generator::ResultSetComparator(sort_info, types);
  std::sort(table_ptr_vec.begin(), table_ptr_vec.end(), rsc);
  // rewrite table by table_ptr_vec
  reWriteTable(table_ptr_vec, types);
  is_sorted_ = true;
}

std::vector<std::vector<int8_t*>> CiderBatch::getRowTable() const {
  int row_num = row_num_;
  int col_num = column_num();
  auto types = schema_->getColumnTypes();
  std::vector<std::vector<int8_t*>> table_ptr_vec;
  table_ptr_vec.reserve(row_num);
  for (int i = 0; i < row_num; i++) {
    std::vector<int8_t*> row_vec;
    row_vec.reserve(col_num);
    for (int j = 0; j < col_num; j++) {
      auto type = types[j];
      switch (type.kind_case()) {
        case ::substrait::Type::KindCase::kBool:
        case ::substrait::Type::KindCase::kI8:
          row_vec.push_back((int8_t*)&((int8_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kI16:
          row_vec.push_back((int8_t*)&((int16_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kI32:
          row_vec.push_back((int8_t*)&((int32_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kI64:
        case ::substrait::Type::KindCase::kDate:
          row_vec.push_back((int8_t*)&((int64_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kFp32:
          row_vec.push_back((int8_t*)&((float*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kFp64:
        case ::substrait::Type::KindCase::kDecimal:
          row_vec.push_back((int8_t*)&((double*)(table_ptr_[j]))[i]);
          break;
        default:
          throw std::runtime_error("Not supported type to print value!");
      }
    }
    table_ptr_vec.push_back(row_vec);
  }
  return std::move(table_ptr_vec);
}

std::string CiderBatch::toValueStringRowTable(
    const std::vector<std::vector<int8_t*>>& table_ptr_vec) const {
  int row_num = row_num_;
  int col_num = column_num();
  auto types = schema_->getColumnTypes();
  std::stringstream ss;
  ss << "row num: " << row_num << ", column num: " << col_num << ".\n";
  for (int i = 0; i < row_num; i++) {
    for (int j = 0; j < col_num; j++) {
      auto type = types[j];
      switch (type.kind_case()) {
        case ::substrait::Type::KindCase::kBool:
        case ::substrait::Type::KindCase::kI8:
          ss << *(int8_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kI16:
          ss << *(int16_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kI32:
          ss << *(int32_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kI64:
        case ::substrait::Type::KindCase::kDate:
          ss << *(int64_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kFp32:
          ss << *(float*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kFp64:
        case ::substrait::Type::KindCase::kDecimal:
          ss << *(double*)table_ptr_vec[i][j] << "\t";
          break;
        default:
          throw std::runtime_error("Not supported type to print value!");
      }
    }
    ss << "\n";
  }
  return ss.str();
}

#define SWAP_VALUE(C_TYPE)                          \
  {                                                 \
    C_TYPE* buff = (C_TYPE*)table_ptr_[i];          \
    std::vector<C_TYPE> tmp;                        \
    tmp.reserve(row_num);                           \
    for (int j = 0; j < row_num; j++) {             \
      tmp.push_back(*(C_TYPE*)table_ptr_vec[j][i]); \
    }                                               \
    for (int k = 0; k < row_num; k++) {             \
      buff[k] = tmp[k];                             \
    }                                               \
    break;                                          \
  }

void CiderBatch::reWriteTable(const std::vector<std::vector<int8_t*>>& table_ptr_vec,
                              const std::vector<substrait::Type>& types) {
  int row_num = row_num_;
  int col_num = column_num();
  for (int i = 0; i < col_num; i++) {
    const auto& type = types[i];
    switch (type.kind_case()) {
      case ::substrait::Type::KindCase::kBool:
      case ::substrait::Type::KindCase::kI8:
        SWAP_VALUE(int8_t)
      case ::substrait::Type::KindCase::kI16:
        SWAP_VALUE(int16_t)
      case ::substrait::Type::KindCase::kI32:
        SWAP_VALUE(int32_t)
      case ::substrait::Type::KindCase::kI64:
      case ::substrait::Type::KindCase::kDate:
        SWAP_VALUE(int64_t)
      case ::substrait::Type::KindCase::kFp32:
        SWAP_VALUE(float)
      case ::substrait::Type::KindCase::kFp64:
      case ::substrait::Type::KindCase::kDecimal:
        SWAP_VALUE(double)
      case ::substrait::Type::KindCase::kFixedChar:
      case ::substrait::Type::KindCase::kVarchar:
      case ::substrait::Type::KindCase::kString: {
        break;
      }
      default:
        throw std::runtime_error("Not supported type to print value!");
    }
  }
}

void CiderBatch::sort(const SortInfo& sort_info) {
  if (is_sorted_ || sort_info.order_entries.size() == 0) {
    return;
  }
  // get result table
  int row_num = row_num_;
  int col_num = column_num();
  auto types = schema_->getColumnTypes();
  std::vector<std::vector<int8_t*>> table_ptr_vec = getTableVec();
  // sort result table
  generator::ResultSetComparator rsc = generator::ResultSetComparator(sort_info, types);
  std::sort(table_ptr_vec.begin(), table_ptr_vec.end(), rsc);
  // rewrite table by table_ptr_vec
  reWriteTable(table_ptr_vec, types);
  is_sorted_ = true;
}

std::vector<std::vector<int8_t*>> CiderBatch::getTableVec() {
  int row_num = row_num_;
  int col_num = column_num();
  auto types = schema_->getColumnTypes();
  std::vector<std::vector<int8_t*>> table_ptr_vec;
  table_ptr_vec.reserve(row_num);
  for (int i = 0; i < row_num; i++) {
    std::vector<int8_t*> row_vec;
    row_vec.reserve(col_num);
    for (int j = 0; j < col_num; j++) {
      auto type = types[j];
      switch (type.kind_case()) {
        case ::substrait::Type::KindCase::kBool:
        case ::substrait::Type::KindCase::kI8:
          row_vec.push_back((int8_t*)&((int8_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kI16:
          row_vec.push_back((int8_t*)&((int16_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kI32:
          row_vec.push_back((int8_t*)&((int32_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kI64:
        case ::substrait::Type::KindCase::kDate:
          row_vec.push_back((int8_t*)&((int64_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kFp32:
          row_vec.push_back((int8_t*)&((float*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kFp64:
        case ::substrait::Type::KindCase::kDecimal:
          row_vec.push_back((int8_t*)&((double*)(table_ptr_[j]))[i]);
          break;
        default:
          throw std::runtime_error("Not supported type to print value!");
      }
    }
    table_ptr_vec.push_back(row_vec);
  }
  return std::move(table_ptr_vec);
}

void CiderBatch::printTable(const std::vector<std::vector<int8_t*>>& table_ptr_vec) {
  int row_num = row_num_;
  int col_num = column_num();
  auto types = schema_->getColumnTypes();
  std::stringstream ss;
  ss << "row num: " << row_num << ", column num: " << col_num << ".\n";
  for (int i = 0; i < row_num; i++) {
    for (int j = 0; j < col_num; j++) {
      auto type = types[j];
      switch (type.kind_case()) {
        case ::substrait::Type::KindCase::kBool:
        case ::substrait::Type::KindCase::kI8:
          ss << *(int8_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kI16:
          ss << *(int16_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kI32:
          ss << *(int32_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kI64:
        case ::substrait::Type::KindCase::kDate:
          ss << *(int64_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kFp32:
          ss << *(float*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kFp64:
        case ::substrait::Type::KindCase::kDecimal:
          ss << *(double*)table_ptr_vec[i][j] << "\t";
          break;
        default:
          throw std::runtime_error("Not supported type to print value!");
      }
    }
    ss << "\n";
  }
  std::cout << ss.str() << std::endl;
}

#define SWAP_VALUE(C_TYPE)                          \
  {                                                 \
    C_TYPE* buff = (C_TYPE*)table_ptr_[i];          \
    std::vector<C_TYPE> tmp;                        \
    tmp.reserve(row_num);                           \
    for (int j = 0; j < row_num; j++) {             \
      tmp.push_back(*(C_TYPE*)table_ptr_vec[j][i]); \
    }                                               \
    for (int k = 0; k < row_num; k++) {             \
      buff[k] = tmp[k];                             \
    }                                               \
    break;                                          \
  }

void CiderBatch::reWriteTable(const std::vector<std::vector<int8_t*>>& table_ptr_vec,
                              const std::vector<substrait::Type>& types) {
  int row_num = row_num_;
  int col_num = column_num();
  for (int i = 0; i < col_num; i++) {
    const auto& type = types[i];
    switch (type.kind_case()) {
      case ::substrait::Type::KindCase::kBool:
      case ::substrait::Type::KindCase::kI8:
        SWAP_VALUE(int8_t)
      case ::substrait::Type::KindCase::kI16:
        SWAP_VALUE(int16_t)
      case ::substrait::Type::KindCase::kI32:
        SWAP_VALUE(int32_t)
      case ::substrait::Type::KindCase::kI64:
      case ::substrait::Type::KindCase::kDate:
        SWAP_VALUE(int64_t)
      case ::substrait::Type::KindCase::kFp32:
        SWAP_VALUE(float)
      case ::substrait::Type::KindCase::kFp64:
      case ::substrait::Type::KindCase::kDecimal:
        SWAP_VALUE(double)
      case ::substrait::Type::KindCase::kFixedChar:
      case ::substrait::Type::KindCase::kVarchar:
      case ::substrait::Type::KindCase::kString: {
        break;
      }
      default:
        throw std::runtime_error("Not supported type to print value!");
    }
  }
}

void CiderBatch::sort(const SortInfo& sort_info) {
  if (is_sorted_ || sort_info.order_entries.size() == 0) {
    return;
  }
  // get result table
  int row_num = row_num_;
  int col_num = column_num();
  auto types = schema_->getColumnTypes();
  std::vector<std::vector<int8_t*>> table_ptr_vec = getRowTable();
  // sort result table
  generator::ResultSetComparator rsc = generator::ResultSetComparator(sort_info, types);
  std::sort(table_ptr_vec.begin(), table_ptr_vec.end(), rsc);
  // rewrite table by table_ptr_vec
  reWriteTable(table_ptr_vec, types);
  is_sorted_ = true;
}

std::vector<std::vector<int8_t*>> CiderBatch::getRowTable() const {
  int row_num = row_num_;
  int col_num = column_num();
  auto types = schema_->getColumnTypes();
  std::vector<std::vector<int8_t*>> table_ptr_vec;
  table_ptr_vec.reserve(row_num);
  for (int i = 0; i < row_num; i++) {
    std::vector<int8_t*> row_vec;
    row_vec.reserve(col_num);
    for (int j = 0; j < col_num; j++) {
      auto type = types[j];
      switch (type.kind_case()) {
        case ::substrait::Type::KindCase::kBool:
        case ::substrait::Type::KindCase::kI8:
          row_vec.push_back((int8_t*)&((int8_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kI16:
          row_vec.push_back((int8_t*)&((int16_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kI32:
          row_vec.push_back((int8_t*)&((int32_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kI64:
        case ::substrait::Type::KindCase::kDate:
          row_vec.push_back((int8_t*)&((int64_t*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kFp32:
          row_vec.push_back((int8_t*)&((float*)(table_ptr_[j]))[i]);
          break;
        case ::substrait::Type::KindCase::kFp64:
        case ::substrait::Type::KindCase::kDecimal:
          row_vec.push_back((int8_t*)&((double*)(table_ptr_[j]))[i]);
          break;
        default:
          throw std::runtime_error("Not supported type to print value!");
      }
    }
    table_ptr_vec.push_back(row_vec);
  }
  return std::move(table_ptr_vec);
}

std::string CiderBatch::toValueStringRowTable(
    const std::vector<std::vector<int8_t*>>& table_ptr_vec) const {
  int row_num = row_num_;
  int col_num = column_num();
  auto types = schema_->getColumnTypes();
  std::stringstream ss;
  ss << "row num: " << row_num << ", column num: " << col_num << ".\n";
  for (int i = 0; i < row_num; i++) {
    for (int j = 0; j < col_num; j++) {
      auto type = types[j];
      switch (type.kind_case()) {
        case ::substrait::Type::KindCase::kBool:
        case ::substrait::Type::KindCase::kI8:
          ss << *(int8_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kI16:
          ss << *(int16_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kI32:
          ss << *(int32_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kI64:
        case ::substrait::Type::KindCase::kDate:
          ss << *(int64_t*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kFp32:
          ss << *(float*)table_ptr_vec[i][j] << "\t";
          break;
        case ::substrait::Type::KindCase::kFp64:
        case ::substrait::Type::KindCase::kDecimal:
          ss << *(double*)table_ptr_vec[i][j] << "\t";
          break;
        default:
          throw std::runtime_error("Not supported type to print value!");
      }
    }
    ss << "\n";
  }
  return ss.str();
}

#define SWAP_VALUE(C_TYPE)                          \
  {                                                 \
    C_TYPE* buff = (C_TYPE*)table_ptr_[i];          \
    std::vector<C_TYPE> tmp;                        \
    tmp.reserve(row_num);                           \
    for (int j = 0; j < row_num; j++) {             \
      tmp.push_back(*(C_TYPE*)table_ptr_vec[j][i]); \
    }                                               \
    for (int k = 0; k < row_num; k++) {             \
      buff[k] = tmp[k];                             \
    }                                               \
    break;                                          \
  }

void CiderBatch::reWriteTable(const std::vector<std::vector<int8_t*>>& table_ptr_vec,
                              const std::vector<substrait::Type>& types) {
  int row_num = row_num_;
  int col_num = column_num();
  for (int i = 0; i < col_num; i++) {
    const auto& type = types[i];
    switch (type.kind_case()) {
      case ::substrait::Type::KindCase::kBool:
      case ::substrait::Type::KindCase::kI8:
        SWAP_VALUE(int8_t)
      case ::substrait::Type::KindCase::kI16:
        SWAP_VALUE(int16_t)
      case ::substrait::Type::KindCase::kI32:
        SWAP_VALUE(int32_t)
      case ::substrait::Type::KindCase::kI64:
      case ::substrait::Type::KindCase::kDate:
        SWAP_VALUE(int64_t)
      case ::substrait::Type::KindCase::kFp32:
        SWAP_VALUE(float)
      case ::substrait::Type::KindCase::kFp64:
      case ::substrait::Type::KindCase::kDecimal:
        SWAP_VALUE(double)
      case ::substrait::Type::KindCase::kFixedChar:
      case ::substrait::Type::KindCase::kVarchar:
      case ::substrait::Type::KindCase::kString: {
        break;
      }
      default:
        throw std::runtime_error("Not supported type to print value!");
    }
  }
}
