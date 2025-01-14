/*
 * Copyright(c) 2022-2023 Intel Corporation.
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

#include "tests/utils/CiderBenchmarkRunner.h"

#include "CiderBenchmarkBase.h"

class CiderOpBenchmark : public CiderBenchmarkBaseFixture {
 public:
  CiderOpBenchmark() {
    runner.prepare(
        "CREATE TABLE test(id1 VARCHAR NOT NULL, id2 VARCHAR NOT NULL, id3 VARCHAR "
        "NOT NULL, id4 BIGINT NOT NULL, id5 BIGINT NOT NULL, id6 BIGINT NOT NULL, "
        "v1 BIGINT NOT NULL, v2 BIGINT NOT NULL,v3 DOUBLE NOT NULL);");
  }

  std::shared_ptr<CiderBatch> input_batch;
};

char* groupbysql("SELECT sum(v1),sum(v2),sum(v3) FROM test group by id6");
// // need to upload dataset
// GEN_BENCHMARK_FROM_FILE(CiderOpBenchmark,
//                         h2oai_q5,
//                         CSVToArrowDataReader,
//                         "/data/G1_1e7_1e2_0_0.csv",
//                         groupbysql);

// // pass set<std::string> col_names, need to be consistent with the create_ddl
// std::unordered_set<std::string> col_names =
//     {"id1", "id2", "id3", "id4", "id5", "id6", "v1", "v2", "v3"};
// GEN_BENCHMARK_FROM_FILE_WITH_COL(CiderOpBenchmark,
//                                  h2oai_q5_custom,
//                                  CSVToArrowDataReader,
//                                  "/data/G1_1e7_1e2_0_0.csv",
//                                  groupbysql,
//                                  col_names);
// Run the benchmark
BENCHMARK_MAIN();
