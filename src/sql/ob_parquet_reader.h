/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <string>
#include <vector>
#include "lib/ob_define.h"
#include "sql/ob_file_sql_utils.h"

namespace oceanbase { namespace sql {

class ObParquetReader
{
public:
  ObParquetReader();
  ~ObParquetReader();
  int open(const std::string &path, const std::vector<int64_t> &projected_columns = {});
  int get_schema(std::vector<ObFileTableColumn> &columns) const;
  int next(std::vector<std::string> &values, std::vector<bool> &nulls);
  int rescan();
  void close();
private:
  struct Impl;
  Impl *impl_;
};

} } // oceanbase::sql
