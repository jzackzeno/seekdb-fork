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

#include <fstream>
#include <string>
#include <vector>
#include "lib/ob_define.h"
#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase { namespace sql {

class ObParquetReader;

enum ObFileTableFormat { FILE_FORMAT_CSV, FILE_FORMAT_JSONL, FILE_FORMAT_PARQUET, FILE_FORMAT_GLOB };

struct ObFileTableOptions
{
  ObFileTableOptions();
  std::string path_;
  char delimiter_;
  char quote_;
  char escape_;
  bool header_;
  std::string null_string_;
  std::string format_;
  int64_t sample_size_;
};

struct ObFileTableColumn
{
  ObFileTableColumn() : type_(common::ObNullType), max_length_(0), has_null_(false) {}
  std::string name_;
  common::ObObjType type_;
  int64_t max_length_;
  // True if any inferred row was NULL / missing for this column. Needed by
  // DESCRIBE SELECT, whose expected output marks day/created as YES and
  // the other basic.csv columns as NO.
  bool has_null_;
};

struct ObFileSnapshot
{
  ObFileSnapshot() : size_(0), mtime_(0), mtime_nsec_(0) {}
  int64_t size_;
  int64_t mtime_;
  int64_t mtime_nsec_;
};

class ObFileSqlUtils
{
public:
  static bool is_file_expr_type(ObExprOperatorType type);
  static ObFileTableFormat format_from_expr_type(ObExprOperatorType type);
  static int parse_options(const std::vector<std::string> &args, ObFileTableOptions &options);
  static int canonicalize_path(const std::string &path, std::string &canonical_path);
  static int check_secure_path(const std::string &secure_root, const std::string &path, bool pattern);
  static int get_file_snapshot(const std::string &path, ObFileSnapshot &snapshot);
  static int infer_schema(ObFileTableFormat format, const ObFileTableOptions &options,
                          std::vector<ObFileTableColumn> &columns, ObFileSnapshot &snapshot);
  static common::ObObjType widen_type(common::ObObjType lhs, common::ObObjType rhs);
  static common::ObObjType infer_scalar_type(const std::string &value, bool json_string);
  static bool parse_date(const std::string &value, int32_t &days);
  static bool parse_datetime(const std::string &value, int64_t &usec);
};

class ObFileTableScanner
{
public:
  ObFileTableScanner();
  ~ObFileTableScanner();
  int open(ObFileTableFormat format, const ObFileTableOptions &options,
           const std::vector<ObFileTableColumn> &columns,
           const std::vector<int64_t> &projected_columns = {});
  int next(std::vector<std::string> &values, std::vector<bool> &is_null);
  int rescan();
  void close();
private:
  int next_csv(std::vector<std::string> &values, std::vector<bool> &is_null);
  int next_json(std::vector<std::string> &values, std::vector<bool> &is_null);
  int next_glob(std::vector<std::string> &values, std::vector<bool> &is_null);
  int read_csv_record(std::vector<std::string> &record);
  int check_unchanged() const;
  ObFileTableFormat format_;
  ObFileTableOptions options_;
  std::vector<ObFileTableColumn> columns_;
  std::ifstream stream_;
  ObFileSnapshot snapshot_;
  int64_t row_number_;
  std::vector<std::string> glob_files_;
  std::vector<int64_t> projected_columns_;
  size_t glob_pos_;
  ObParquetReader *parquet_reader_;
};

} } // oceanbase::sql
