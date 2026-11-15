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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_explain_log_plan.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/code_generator/ob_code_generator.h"
#include "sql/ob_file_sql_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::sql::log_op_def;

/**
 *  This function does two things:
 *
 *  1. generate a logical plan for the 'real' query that's being 'explained'
 *  2. generate the plan text from the logical plan and put the text in the buffer
 *     and remember the logical plan as well.
 */
int ObExplainLogPlan::generate_normal_raw_plan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_explain_stmt())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObLogPlan *child_plan = NULL;
    const ObDMLStmt *child_stmt = NULL;
    ObLogicalOperator *top = NULL;
    ObLogValues *values_op = NULL;
    int64_t batch_size = 0;
    const ObExplainStmt *explain_stmt = static_cast<const ObExplainStmt*>(get_stmt());
    if (OB_ISNULL(child_stmt = explain_stmt->get_explain_query_stmt())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(child_plan = optimizer_context_.get_log_plan_factory().
                         create(optimizer_context_, *child_stmt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create log plan for explain stmt");
    } else if (OB_FAIL(child_plan->generate_plan())) {
    } else if (OB_FAIL(remove_duplicate_constraints())) {
    } else if (OB_FAIL(check_explain_generate_plan_with_outline(child_plan))) {
    } else if (OB_FAIL(ObCodeGenerator::detect_batch_size(*child_plan, batch_size))) {
    } else if (OB_FAIL(allocate_values_as_top(top))) {
    } else if (OB_ISNULL(top) || OB_UNLIKELY(LOG_VALUES != top->get_type())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      set_plan_root(top);
      top->mark_is_plan_root();
      child_plan->get_optimizer_context().set_batch_size(batch_size);
      values_op = static_cast<ObLogValues*>(top);
      values_op->set_explain_plan(child_plan);
      const bool is_describe = (EXPLAIN_DESCRIBE_SELECT == explain_stmt->get_explain_type());
      if (is_describe) {
        if (OB_FAIL(build_describe_rows(child_stmt, *values_op))) {
          LOG_WARN("failed to build describe rows", K(ret));
        }
      } else {
      ObSqlPlan sql_plan(get_allocator());
      sql_plan.set_session_info(get_optimizer_context().get_session_info());
      ObExplainLogPlan *explain_plan = static_cast<ObExplainLogPlan*>(child_plan);
      ObSEArray<common::ObString, 64> plan_strs;
      const ObString& into_table = explain_stmt->get_into_table();
      const ObString& statement_id = explain_stmt->get_statement_id();
      if (OB_FAIL(sql_plan.store_sql_plan_for_explain(get_optimizer_context().get_exec_ctx(),
                                                      child_plan,
                                                      explain_stmt->get_explain_type(),
                                                      0 == into_table.length() ? "PLAN_TABLE" : into_table,
                                                      0 == statement_id.length() ? "" : statement_id,
                                                      explain_stmt->get_display_opt(),
                                                      plan_strs))) {
      } else {
        //For explain stmt, we can do pack at the stage of expr alloc,
        //But we need to use the FALSE flag to tell the driver
        //to use normal encoding instead of memcopy
        optimizer_context_.set_packed(false);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < plan_strs.count(); ++i) {
        ObObj obj;
        obj.set_varchar(plan_strs.at(i));
        ObNewRow row;
        row.cells_ = &obj;
        row.count_ = 1;
        if (OB_FAIL(values_op->add_row(row))) {
        }
      }
      }
      if (OB_SUCC(ret)) {
        // set values_op operator id && set max operator id for LogPlan
        values_op->set_op_id(0);
        set_max_op_id(1);
      }
    }
    get_optimizer_context().get_all_exprs().reuse();
    if (OB_FAIL(ret)) {
    } else if (NULL == values_op) {
      ret = OB_ERR_UNEXPECTED;
    } else if (EXPLAIN_DESCRIBE_SELECT == explain_stmt->get_explain_type()
               && OB_FAIL(allocate_describe_output_exprs(*values_op))) {
    } else if (EXPLAIN_DESCRIBE_SELECT != explain_stmt->get_explain_type()
               && OB_FAIL(allocate_output_expr_for_values_op(*values_op))) {
    } else {
      get_optimizer_context().set_plan_type(ObPhyPlanType::OB_PHY_PLAN_LOCAL,
                                            ObPhyPlanType::OB_PHY_PLAN_LOCAL,
                                            false);
    }
  }
  return ret;
}

static const char *describe_type_name(common::ObObjType type)
{
  switch (type) {
    case common::ObIntType: return "BIGINT";
    case common::ObUIntType: return "BIGINT";
    case common::ObTinyIntType: return "BOOLEAN";
    case common::ObDoubleType: return "DOUBLE";
    case common::ObFloatType: return "DOUBLE";
    case common::ObVarcharType: return "VARCHAR";
    case common::ObDateType: return "DATE";
    case common::ObDateTimeType: return "DATETIME";
    case common::ObNullType: return "VARCHAR";
    default: return "VARCHAR";
  }
}

int ObExplainLogPlan::build_describe_rows(const ObDMLStmt *child_stmt, ObLogValues &values_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_stmt) || !child_stmt->is_select_stmt()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "DESCRIBE is only supported for SELECT");
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt *>(child_stmt);
    const common::ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    std::vector<ObFileTableColumn> file_columns;
    bool use_file_columns = false;
    // Fast path: SELECT * FROM a single file table. Re-infer the schema so
    // the rows carry the exact inferred types and nullability (has_null_)
    // that DESCRIBE is expected to report, e.g. day/created = YES.
    const common::ObIArray<TableItem *> &table_items = child_stmt->get_table_items();
    for (int64_t t = 0; !use_file_columns && t < table_items.count(); ++t) {
      TableItem *table_item = table_items.at(t);
      if (NULL == table_item || !table_item->is_function_table()
          || NULL == table_item->function_table_expr_) {
        continue;
      }
      ObRawExpr *table_expr = table_item->function_table_expr_;
      if (!ObFileSqlUtils::is_file_expr_type(table_expr->get_expr_type())) {
        continue;
      }
      std::vector<std::string> args;
      ObFileTableOptions options;
      ObFileSnapshot snapshot;
      for (int64_t i = 0; OB_SUCC(ret) && i < table_expr->get_param_count(); ++i) {
        ObRawExpr *param = table_expr->get_param_expr(i);
        const ObRawExpr *real_param = NULL;
        if (OB_ISNULL(param)) {
          ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(param, real_param))) {
        } else if (OB_ISNULL(real_param) || !real_param->is_const_raw_expr()) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          const ObConstRawExpr *const_param = static_cast<const ObConstRawExpr *>(real_param);
          const ObObj &obj = real_param->is_param_expr()
                           ? const_param->get_param()
                           : const_param->get_value();
          if (obj.is_string_type()) {
            const ObString str = obj.get_string();
            args.push_back(std::string(str.ptr(), str.length()));
          } else if (obj.is_uint64()) {
            args.push_back(std::to_string(obj.get_uint64()));
          } else if (obj.is_integer_type()) {
            args.push_back(std::to_string(obj.get_int()));
          } else {
            ret = OB_INVALID_ARGUMENT;
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(ObFileSqlUtils::parse_options(args, options))) {
        // Fall back to the select items below instead of failing DESCRIBE.
        ret = OB_SUCCESS;
      } else if (OB_SUCC(ret)
                 && OB_FAIL(ObFileSqlUtils::infer_schema(
                      ObFileSqlUtils::format_from_expr_type(table_expr->get_expr_type()),
                      options, file_columns, snapshot))) {
        file_columns.clear();
        ret = OB_SUCCESS;
      } else if (OB_SUCC(ret) && !file_columns.empty()
                 && file_columns.size() == static_cast<size_t>(select_items.count())) {
        use_file_columns = true;
      }
    }
    if (OB_SUCC(ret) && use_file_columns) {
      for (size_t i = 0; OB_SUCC(ret) && i < file_columns.size(); ++i) {
        ObObj cells[6];
        cells[0].set_varchar(ObString(static_cast<int32_t>(file_columns[i].name_.length()),
                                      file_columns[i].name_.data()));
        const char *type_str = describe_type_name(file_columns[i].type_);
        cells[1].set_varchar(ObString::make_string(type_str));
        cells[2].set_varchar(ObString(file_columns[i].has_null_ ? 3 : 2,
                                      file_columns[i].has_null_ ? "YES" : "NO"));
        cells[3].set_null();
        cells[4].set_null();
        cells[5].set_null();
        ObNewRow row;
        row.cells_ = cells;
        row.count_ = 6;
        if (OB_FAIL(values_op.add_row(row))) {
        }
      }
    } else if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
        const SelectItem &item = select_items.at(i);
        if (OB_ISNULL(item.expr_)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          ObString col_name = item.alias_name_;
          char buf[128];
          if (0 == col_name.length()) {
            col_name = item.expr_name_;
          }
          if (0 == col_name.length()) {
            int64_t pos = 0;
            if (OB_SUCC(item.expr_->get_name(buf, sizeof(buf), pos))) {
              col_name.assign_ptr(buf, static_cast<int32_t>(pos));
            }
          }
          const char *type_str = describe_type_name(item.expr_->get_result_type().get_type());
          ObObj cells[6];
          cells[0].set_varchar(col_name);
          cells[1].set_varchar(ObString::make_string(type_str));
          cells[2].set_varchar(ObString(item.expr_->is_not_null_for_read() ? 2 : 3,
                                        item.expr_->is_not_null_for_read() ? "NO" : "YES"));
          cells[3].set_null();
          cells[4].set_null();
          cells[5].set_null();
          ObNewRow row;
          row.cells_ = cells;
          row.count_ = 6;
          if (OB_FAIL(values_op.add_row(row))) {
          }
        }
      }
    }
  }
  return ret;
}

int ObExplainLogPlan::allocate_describe_output_exprs(ObLogValues &values_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(optimizer_context_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Six VARCHAR output columns: column_name, column_type, null, key,
    // default, extra. Mirrors ObLogPlan::allocate_output_expr_for_values_op
    // (single column) but with one const expr per describe column, so the
    // Values operator emits 6 cells per row.
    for (int64_t i = 0; OB_SUCC(ret) && i < 6; ++i) {
      ObConstRawExpr *output = NULL;
      if (OB_FAIL(optimizer_context_.get_expr_factory().create_raw_expr(T_VARCHAR, output))) {
      } else {
        ObObj v;
        v.set_varchar(" ");
        v.set_collation_type(ObCharset::get_system_collation());
        output->set_value(v);
        if (OB_FAIL(output->formalize(optimizer_context_.get_session_info()))) {
        } else if (OB_FAIL(values_op.get_output_exprs().push_back(output))) {
        } else if (OB_FAIL(get_optimizer_context().get_all_exprs().append(output))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      values_op.set_branch_id(0);
      values_op.set_id(0);
      values_op.set_op_id(0);
    }
  }
  return ret;
}

int ObExplainLogPlan::check_explain_generate_plan_with_outline(ObLogPlan *real_plan)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  ObSqlCtx *sql_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  const ObExplainStmt *explain_stmt = static_cast<const ObExplainStmt*>(get_stmt());
  ObQueryCtx *query_ctx = get_optimizer_context().get_query_ctx();
  if (OB_ISNULL(real_plan) || OB_ISNULL(explain_stmt)
      || OB_ISNULL(exec_ctx = get_optimizer_context().get_exec_ctx())
      || OB_ISNULL(session_info = get_optimizer_context().get_session_info())
      || OB_ISNULL(sql_ctx = exec_ctx->get_sql_ctx()) || OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (session_info->is_inner() || sql_ctx->is_prepare_protocol_) {
    /* do not check explain for inner sql (include query in PL) */
  } else if (OB_UNLIKELY(get_optimizer_context().get_query_ctx() != NULL
                         && get_optimizer_context().get_query_ctx()->get_injected_random_status())) { 
    /* do nothing */
  } else if (sql_ctx->multi_stmt_item_.is_part_of_multi_stmt()
             && sql_ctx->multi_stmt_item_.get_seq_num() > 0) {
    /* generate plan call by ObMPQuery::process_with_tmp_context use tmp context, do not check */
  } else if (EXPLAIN_UNINITIALIZED !=  explain_stmt->get_explain_type()
             && EXPLAIN_BASIC !=  explain_stmt->get_explain_type()
             && EXPLAIN_OUTLINE !=  explain_stmt->get_explain_type()) {
    /* generate plan again for explain/explain basic/explain outline,
      do not check explain extended/explain extended_noaddr */
  } else if (0 == sql_ctx->first_plan_hash_) {  /* generate plan first time */
    void *tmp_ptr = NULL;
    sql_ctx->first_outline_data_.reset();
    if (OB_UNLIKELY(0 == real_plan->get_signature()) || 0 < sql_ctx->retry_times_) {
      /* do nothing */
    } else if (OB_SUCC(OB_E(EventTable::EN_EXPLAIN_GENERATE_PLAN_WITH_OUTLINE) OB_SUCCESS)) {
      /* do nothing */
    } else if (OB_UNLIKELY(NULL == (tmp_ptr = get_optimizer_context().get_allocator().alloc(OB_MAX_SQL_LENGTH)))) {
      /* allocator in optimizer context is from ObResultSet */
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory", K(ret));
    } else {
      PlanText plan_text;
      plan_text.buf_ = static_cast<char *>(tmp_ptr);
      plan_text.buf_len_ = OB_MAX_SQL_LENGTH;
      if (OB_FAIL(ObSqlPlan::get_plan_outline_info_one_line(plan_text, real_plan))) {
      } else {
        sql_ctx->first_outline_data_.assign_ptr(plan_text.buf_, static_cast<ObString::obstr_size_t>(plan_text.pos_));
        sql_ctx->first_plan_hash_ = real_plan->get_signature();
        sql_ctx->first_equal_param_cons_cnt_ = query_ctx->all_equal_param_constraints_.count();
        sql_ctx->first_const_param_cons_cnt_ = query_ctx->all_plan_const_param_constraints_.count();
        sql_ctx->first_expr_cons_cnt_ = query_ctx->all_expr_constraints_.count();
        ret = OB_SQL_RETRY_OUTLINE;
      }
    }
  } else {  /* check generate plan again use outline data */
    SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
      PlanText plan_text;
      plan_text.buf_ = buf;
      plan_text.buf_len_ = OB_MAX_SQL_LENGTH;
      if (OB_FAIL(ObSqlPlan::get_plan_outline_info_one_line(plan_text, real_plan))) {
      } else {
        ObString cur_outline_data;
        const uint64_t cur_plan_hash = real_plan->get_signature();
        cur_outline_data.assign_ptr(plan_text.buf_, static_cast<ObString::obstr_size_t>(plan_text.pos_));
        if (cur_plan_hash != sql_ctx->first_plan_hash_) {
          ret = OB_OUTLINE_NOT_REPRODUCIBLE;
          LOG_WARN("failed to generate plan use outline", K(sql_ctx->first_plan_hash_));
        }
        if (0 != (cur_outline_data.case_compare(sql_ctx->first_outline_data_))) {
          ret = OB_OUTLINE_NOT_REPRODUCIBLE;
          LOG_WARN("failed to generate plan use outline", K(sql_ctx->first_outline_data_));
        }
        if (query_ctx->all_equal_param_constraints_.count() != sql_ctx->first_equal_param_cons_cnt_ || 
            query_ctx->all_plan_const_param_constraints_.count() != sql_ctx->first_const_param_cons_cnt_ || 
            query_ctx->all_expr_constraints_.count() != sql_ctx->first_expr_cons_cnt_) {
          ret = OB_OUTLINE_NOT_REPRODUCIBLE;
          LOG_WARN("failed to generate plan use outline", K(sql_ctx->first_equal_param_cons_cnt_), 
                                                          K(sql_ctx->first_const_param_cons_cnt_), 
                                                          K(sql_ctx->first_expr_cons_cnt_),
                                                          K(query_ctx->all_equal_param_constraints_.count()),
                                                          K(query_ctx->all_plan_const_param_constraints_.count()),
                                                          K(query_ctx->all_expr_constraints_.count()));
        }
        if (OB_FAIL(ret)) {
        }
      }
      sql_ctx->first_plan_hash_ = 0;
      sql_ctx->first_outline_data_.reset();
      sql_ctx->first_equal_param_cons_cnt_ = 0;
      sql_ctx->first_const_param_cons_cnt_ = 0;
      sql_ctx->first_expr_cons_cnt_ = 0;
    }
  }
  return ret;
}
