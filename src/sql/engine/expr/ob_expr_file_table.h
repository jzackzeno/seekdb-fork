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

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprFileTable : public ObFuncExprOperator
{
public:
  ObExprFileTable(common::ObIAllocator &alloc,
                  ObExprOperatorType type,
                  const char *name);
  virtual ~ObExprFileTable() = default;
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_not_scalar(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
};

#define DECLARE_FILE_TABLE_EXPR(class_name) \
class class_name : public ObExprFileTable \
{ \
public: \
  explicit class_name(common::ObIAllocator &alloc); \
}

DECLARE_FILE_TABLE_EXPR(ObExprReadCsv);
DECLARE_FILE_TABLE_EXPR(ObExprReadJson);
DECLARE_FILE_TABLE_EXPR(ObExprReadNdjson);
DECLARE_FILE_TABLE_EXPR(ObExprReadParquet);
DECLARE_FILE_TABLE_EXPR(ObExprGlob);

#undef DECLARE_FILE_TABLE_EXPR

inline bool is_file_table_expr_type(const ObExprOperatorType type)
{
  return T_FUN_SYS_READ_CSV == type
      || T_FUN_SYS_READ_JSON == type
      || T_FUN_SYS_READ_NDJSON == type
      || T_FUN_SYS_READ_PARQUET == type
      || T_FUN_SYS_GLOB == type;
}

} // namespace sql
} // namespace oceanbase
