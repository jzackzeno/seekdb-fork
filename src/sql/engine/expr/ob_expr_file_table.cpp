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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_file_table.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprFileTable::ObExprFileTable(ObIAllocator &alloc,
                                 ObExprOperatorType type,
                                 const char *name)
  : ObFuncExprOperator(alloc, type, name, MORE_THAN_ZERO,
                       NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprFileTable::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types) || param_num < 1) {
    ret = OB_ERR_PARAM_SIZE;
  } else {
    for (int64_t i = 0; i < param_num; ++i) {
      types[i].set_calc_type(ObVarcharType);
      types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  }
  return ret;
}

int ObExprFileTable::eval_not_scalar(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObDatum &res)
{
  UNUSED(expr);
  UNUSED(ctx);
  res.set_null();
  return OB_NOT_SUPPORTED;
}

int ObExprFileTable::cg_expr(ObExprCGCtx &expr_cg_ctx,
                             const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_not_scalar;
  return OB_SUCCESS;
}

#define DEFINE_FILE_TABLE_EXPR(class_name, item_type, sql_name) \
class_name::class_name(ObIAllocator &alloc) \
  : ObExprFileTable(alloc, item_type, sql_name) \
{ \
}

DEFINE_FILE_TABLE_EXPR(ObExprReadCsv, T_FUN_SYS_READ_CSV, "read_csv");
DEFINE_FILE_TABLE_EXPR(ObExprReadJson, T_FUN_SYS_READ_JSON, "read_json");
DEFINE_FILE_TABLE_EXPR(ObExprReadNdjson, T_FUN_SYS_READ_NDJSON, "read_ndjson");
DEFINE_FILE_TABLE_EXPR(ObExprReadParquet, T_FUN_SYS_READ_PARQUET, "read_parquet");
DEFINE_FILE_TABLE_EXPR(ObExprGlob, T_FUN_SYS_GLOB, "glob");

#undef DEFINE_FILE_TABLE_EXPR

} // namespace sql
} // namespace oceanbase
