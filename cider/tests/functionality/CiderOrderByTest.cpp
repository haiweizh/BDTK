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

#include <gtest/gtest.h>
#include "CiderTestBase.h"

class CiderOrderByFunctionTest : public CiderTestBase {
 public:
  CiderOrderByFunctionTest() {
    table_name_ = "table_test";
    create_ddl_ =
        "CREATE TABLE table_test(col_a BIGINT, col_b BIGINT, col_c BIGINT, col_d BIGINT, "
        "col_e BIGINT, col_f BIGINT);";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        10,
        {"col_a", "col_b", "col_c", "col_d", "col_e", "col_f"},
        {CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64),
         CREATE_SUBSTRAIT_TYPE(I64)},
        {0, 0, 0, 2, 2, 2},
        GeneratePattern::Random,
        1,
        10))};
  }
};

class CiderStringOrderByTest : public CiderTestBase {
 public:
  CiderStringOrderByTest() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));)";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        10,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)}))};
  }
};

class CiderRandomStringOrderByTest : public CiderTestBase {
 public:
  CiderRandomStringOrderByTest() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));)";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        10,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {},
        GeneratePattern::Random,
        2,
        20))};
  }
};

class CiderNullableStringOrderByTest : public CiderTestBase {
 public:
  CiderNullableStringOrderByTest() {
    table_name_ = "test";
    create_ddl_ = R"(CREATE TABLE test(col_1 INTEGER, col_2 VARCHAR(10));)";
    input_ = {std::make_shared<CiderBatch>(QueryDataGenerator::generateBatchByTypes(
        20,
        {"col_1", "col_2"},
        {CREATE_SUBSTRAIT_TYPE(I32), CREATE_SUBSTRAIT_TYPE(Varchar)},
        {0, 2},
        GeneratePattern::Random,
        2,
        20))};
  }
};

TEST_F(CiderOrderByFunctionTest, SelectColumnOrderByTest) {
  // order by without nulls first/last
  assertQuery("SELECT col_a FROM table_test ORDER BY 1");
  assertQuery("SELECT col_a FROM table_test ORDER BY col_a");
  assertQuery("SELECT col_a FROM table_test ORDER BY col_a ASC");
  assertQuery("SELECT col_a FROM table_test ORDER BY col_a DESC");
  assertQuery("SELECT col_b FROM table_test ORDER BY col_b");
  assertQuery("SELECT col_b FROM table_test ORDER BY col_b ASC");
  assertQuery("SELECT col_b FROM table_test ORDER BY col_b DESC");
  assertQuery("SELECT col_a, col_b FROM table_test ORDER BY col_a, col_b");
  assertQuery("SELECT col_a, col_b FROM table_test ORDER BY col_a ASC, col_b ASC");
  assertQuery("SELECT col_a, col_b FROM table_test ORDER BY col_a DESC, col_b DESC");
  assertQuery("SELECT col_a, col_b FROM table_test ORDER BY col_a ASC, col_b DESC");
  assertQuery("SELECT col_a, col_b FROM table_test ORDER BY col_a DESC, col_b ASC");
  assertQuery("SELECT col_a, col_b FROM table_test ORDER BY col_b");
  assertQuery("SELECT * FROM table_test ORDER BY col_a, col_b");
  assertQuery("SELECT * FROM table_test ORDER BY col_a ASC, col_b ASC");
  assertQuery("SELECT * FROM table_test ORDER BY col_a DESC, col_b DESC");
  assertQuery("SELECT * FROM table_test ORDER BY col_a ASC, col_b DESC");
  assertQuery("SELECT * FROM table_test ORDER BY col_a DESC, col_b ASC");
  // todo: cider batch's col size is wrong.
  // assertQuery("SELECT col_a FROM table_test ORDER BY col_b");
  // assertQuery("SELECT col_a FROM table_test ORDER BY col_b ASC");
  // assertQuery("SELECT col_a FROM table_test ORDER BY col_b DESC");
  // order by with nulls first/last
  assertQuery("SELECT col_b FROM table_test ORDER BY col_b NULLS FIRST");
  assertQuery("SELECT col_b FROM table_test ORDER BY col_b NULLS LAST");
  assertQuery("SELECT col_b FROM table_test ORDER BY col_b ASC NULLS FIRST");
  assertQuery("SELECT col_b FROM table_test ORDER BY col_b ASC NULLS LAST");
  assertQuery("SELECT col_b FROM table_test ORDER BY col_b DESC NULLS FIRST");
  assertQuery("SELECT col_b FROM table_test ORDER BY col_b DESC NULLS LAST");
  assertQuery("SELECT col_d FROM table_test ORDER BY col_d NULLS FIRST");
  assertQuery("SELECT col_d FROM table_test ORDER BY col_d NULLS LAST");
  assertQuery("SELECT col_d FROM table_test ORDER BY col_d ASC NULLS FIRST");
  assertQuery("SELECT col_d FROM table_test ORDER BY col_d ASC NULLS LAST");
  assertQuery("SELECT col_d FROM table_test ORDER BY col_d DESC NULLS FIRST");
  assertQuery("SELECT col_d FROM table_test ORDER BY col_d DESC NULLS LAST");
}

TEST_F(CiderOrderByFunctionTest, AggGroupOrderByTest) {
  assertQuery("SELECT col_a, count(*) FROM table_test GROUP BY col_a ORDER BY 1");
  assertQuery("SELECT col_a, count(*) FROM table_test GROUP BY col_a ORDER BY col_a");
  assertQuery("SELECT col_b, count(*) FROM table_test GROUP BY col_b ORDER BY col_b");
  assertQuery("SELECT col_a, count(*) FROM table_test GROUP BY col_a ORDER BY col_a ASC");
  assertQuery(
      "SELECT col_a, count(*) FROM table_test GROUP BY col_a ORDER BY col_a DESC");
  assertQuery(
      "SELECT col_a, col_b, count(*) FROM table_test GROUP BY col_a, col_b ORDER BY "
      "col_a, col_b");
}

TEST_F(CiderStringOrderByTest, SelectStringColumnOrderByTest) {
  assertQuery("SELECT * FROM test ORDER BY col_1");
  assertQuery("SELECT * FROM test ORDER BY col_2");
  assertQuery("SELECT * FROM test ORDER BY col_2 ASC");
  assertQuery("SELECT * FROM test ORDER BY col_2 DESC");
  assertQuery("SELECT * FROM test ORDER BY col_2 ASC NULLS FIRST");
  assertQuery("SELECT * FROM test ORDER BY col_2 ASC NULLS LAST");
}

TEST_F(CiderRandomStringOrderByTest, SelectRandomStringColumnOrderByTest) {
  assertQuery("SELECT * FROM test ORDER BY col_1");
  assertQuery("SELECT * FROM test ORDER BY col_2");
  assertQuery("SELECT * FROM test ORDER BY col_2 ASC");
  assertQuery("SELECT * FROM test ORDER BY col_2 DESC");
  assertQuery("SELECT * FROM test ORDER BY col_2 ASC NULLS FIRST");
  assertQuery("SELECT * FROM test ORDER BY col_2 ASC NULLS LAST");
}

TEST_F(CiderNullableStringOrderByTest, SelectNullableStringColumnOrderByTest) {
  assertQuery("SELECT * FROM test ORDER BY col_1, col_2 ASC NULLS FIRST");
  assertQuery("SELECT * FROM test ORDER BY col_1, col_2 ASC NULLS LAST");
  assertQuery("SELECT * FROM test ORDER BY col_1, col_2 DESC NULLS FIRST");
  assertQuery("SELECT * FROM test ORDER BY col_1, col_2 DESC NULLS LAST");
  GTEST_SKIP_("Same col1's value, col2's value is random order, that is right");
  assertQuery("SELECT * FROM test ORDER BY col_1");
  assertQuery("SELECT * FROM test ORDER BY col_2");
  assertQuery("SELECT * FROM test ORDER BY col_2 ASC");
  assertQuery("SELECT * FROM test ORDER BY col_2 DESC");
  assertQuery("SELECT * FROM test ORDER BY col_2 ASC NULLS FIRST");
  assertQuery("SELECT * FROM test ORDER BY col_2 ASC NULLS LAST");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  try {
    err = RUN_ALL_TESTS();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  return err;
}
