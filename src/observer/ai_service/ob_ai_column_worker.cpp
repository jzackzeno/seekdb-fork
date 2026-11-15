/* Copyright (c) 2025 OceanBase.
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

#include "observer/ai_service/ob_ai_column_worker.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_sql_string.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_server_struct.h"
#include "observer/omt/ob_ai_service.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

namespace oceanbase
{
namespace observer
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sqlclient;

// Task states in oceanbase.__all_ai_column_task. Only PENDING and
// RETRY_WAIT are written by the DML hook and this worker; RUNNING is
// unused (a tick never overlaps itself, see AiColumnWorkerTask).
static const int64_t AI_TASK_STATE_PENDING = 0;
static const int64_t AI_TASK_STATE_RETRY_WAIT = 3;
// Maximum tasks claimed in one tick.
static const int64_t AI_COLUMN_WORKER_BATCH_LIMIT = 100;
// System prompts derived from the AI column result type. The DDL stores
// the same strings in __all_ai_column.normalized_expr, so the worker
// recovers them by substring instead of re-deriving them from the type.
static const char *AI_COLUMN_JSON_HINT = "Return exactly one valid JSON value";
static const char *AI_COLUMN_INT_HINT = "Return exactly one base-10 integer";

namespace
{

struct AiColumnTaskRow
{
  AiColumnTaskRow()
    : task_id_(0), table_id_(0), column_id_(0), request_id_(0), state_(0),
      retry_count_(0), definition_version_(0)
  {}
  int64_t task_id_;
  int64_t table_id_;
  int64_t column_id_;
  int64_t request_id_;
  int64_t state_;
  int64_t retry_count_;
  int64_t definition_version_;
  ObString rowkey_predicate_;
  ObString prompt_;
};

struct AiColumnDef
{
  AiColumnDef()
    : column_id_(0), max_retries_(0), state_column_id_(0), request_column_id_(0),
      result_spec_column_id_(0), timeout_ms_(0), retry_delay_ms_(0),
      definition_version_(0), func_type_(0)
  {}
  int64_t column_id_;
  int64_t max_retries_;
  int64_t state_column_id_;
  int64_t request_column_id_;
  int64_t result_spec_column_id_;
  int64_t timeout_ms_;
  int64_t retry_delay_ms_;
  int64_t definition_version_;
  // 0 = AI_COMPLETE, 1 = AI_EMBED.
  int64_t func_type_;
  ObString column_name_;
  ObString model_name_;
  ObString normalized_expr_;
  ObString database_name_;
  ObString table_name_;
  ObString state_column_name_;
  ObString request_column_name_;
};

struct AiColumnTaskGroup
{
  AiColumnTaskGroup() : table_id_(0), request_id_(0) {}
  int64_t table_id_;
  int64_t request_id_;
  ObSEArray<int64_t, 4> task_indexes_;
};

// Double single quotes so a model result can be embedded in a SQL string
// literal. The caller wraps the result in single quotes.
int escape_sql_literal(ObIAllocator &alloc, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  ObSqlString buf;
  for (int64_t i = 0; OB_SUCC(ret) && i < src.length(); ++i) {
    const char c = src.ptr()[i];
    if ('\'' == c) {
      if (OB_FAIL(buf.append("''"))) {
      }
    } else if (OB_FAIL(buf.append(&c, 1))) {
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(alloc, buf.string(), dst))) {
  }
  return ret;
}

} // namespace

class AiColumnWorker
{
public:
  static int run_once();
private:
  static int read_due_tasks(ObIAllocator &alloc, ObSEArray<AiColumnTaskRow, 128> &tasks);
  static int read_ai_column_def(ObIAllocator &alloc, int64_t table_id, int64_t column_id,
                                AiColumnDef &def);
  static int call_model(ObIAllocator &alloc, const AiColumnDef &def,
                        const ObString &prompt, ObString &result);
  static int publish_group(ObMySQLTransaction &trans,
                           const ObSEArray<AiColumnTaskRow, 128> &tasks,
                           const AiColumnTaskGroup &group,
                           const ObSEArray<AiColumnDef, 4> &defs,
                           const ObSEArray<ObString, 4> &results,
                           ObIAllocator &alloc);
  static int fail_group(ObMySQLTransaction &trans,
                        const ObSEArray<AiColumnTaskRow, 128> &tasks,
                        const AiColumnTaskGroup &group,
                        const ObSEArray<AiColumnDef, 4> &defs);
  static int process_group(ObIAllocator &alloc,
                           const ObSEArray<AiColumnTaskRow, 128> &tasks,
                           const AiColumnTaskGroup &group);
};

AiColumnWorkerTask::AiColumnWorkerTask() : running_(false)
{}

AiColumnWorkerTask::~AiColumnWorkerTask()
{}

void AiColumnWorkerTask::runTimerTask()
{
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    // Previous tick is still calling a model; skip this tick.
    return;
  }
  int ret = AiColumnWorker::run_once();
  if (OB_FAIL(ret)) {
    LOG_WARN("ai column worker tick failed", K(ret));
  }
  running_.store(false);
}

int AiColumnWorker::read_due_tasks(ObIAllocator &alloc, ObSEArray<AiColumnTaskRow, 128> &tasks)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT task_id, table_id, column_id, request_id, rowkey_predicate, "
            "state, retry_count, eligible_ts, prompt, definition_version "
            "FROM oceanbase.__all_ai_column_task "
            "WHERE state IN (%ld, %ld) AND eligible_ts <= now(6) "
            "ORDER BY task_id LIMIT %ld",
            AI_TASK_STATE_PENDING, AI_TASK_STATE_RETRY_WAIT,
            AI_COLUMN_WORKER_BATCH_LIMIT))) {
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy->read(res, sql.ptr()))) {
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              }
            } else {
              AiColumnTaskRow row;
              ObObj obj;
              if (OB_FAIL(result->get_obj("task_id", obj))) {
              } else if (OB_FAIL(obj.get_int(row.task_id_))) {
              } else if (OB_FAIL(result->get_obj("table_id", obj))) {
              } else if (OB_FAIL(obj.get_int(row.table_id_))) {
              } else if (OB_FAIL(result->get_obj("column_id", obj))) {
              } else if (OB_FAIL(obj.get_int(row.column_id_))) {
              } else if (OB_FAIL(result->get_obj("request_id", obj))) {
              } else if (OB_FAIL(obj.get_int(row.request_id_))) {
              } else if (OB_FAIL(result->get_obj("state", obj))) {
              } else if (OB_FAIL(obj.get_int(row.state_))) {
              } else if (OB_FAIL(result->get_obj("retry_count", obj))) {
              } else if (OB_FAIL(obj.get_int(row.retry_count_))) {
              } else if (OB_FAIL(result->get_obj("definition_version", obj))) {
              } else if (OB_FAIL(obj.get_int(row.definition_version_))) {
              } else if (OB_FAIL(result->get_obj("rowkey_predicate", obj))) {
              } else if (OB_FAIL(obj.get_string(row.rowkey_predicate_))) {
              } else if (OB_FAIL(result->get_obj("prompt", obj))) {
              } else if (!obj.is_null() && OB_FAIL(obj.get_string(row.prompt_))) {
              } else if (OB_FAIL(ob_write_string(alloc, row.rowkey_predicate_,
                                                 row.rowkey_predicate_))) {
              } else if (OB_FAIL(ob_write_string(alloc, row.prompt_, row.prompt_))) {
              } else if (OB_FAIL(tasks.push_back(row))) {
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int AiColumnWorker::read_ai_column_def(ObIAllocator &alloc, int64_t table_id,
                                       int64_t column_id, AiColumnDef &def)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT c.column_name, c.max_retries, c.state_column_id, "
            "c.request_column_id, c.result_spec_column_id, c.normalized_expr, "
            "c.timeout_ms, c.retry_delay_ms, c.model_name, c.func_type, "
            "c.definition_version, d.database_name, t.table_name, "
            "hs.column_name AS state_column_name, hr.column_name AS request_column_name "
            "FROM oceanbase.__all_ai_column c "
            "JOIN oceanbase.__all_table t ON t.table_id = c.table_id "
            "JOIN oceanbase.__all_database d ON d.database_id = t.database_id "
            "JOIN oceanbase.__all_column hs ON hs.table_id = c.table_id "
            "AND hs.column_id = c.state_column_id "
            "JOIN oceanbase.__all_column hr ON hr.table_id = c.table_id "
            "AND hr.column_id = c.request_column_id "
            "WHERE c.table_id = %ld AND c.column_id = %ld",
            table_id, column_id))) {
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy->read(res, sql.ptr()))) {
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            // AI column (or table) is gone; the caller drops the task.
            ret = OB_ENTRY_NOT_EXIST;
          }
        } else {
          ObObj obj;
          if (OB_FAIL(result->get_obj("column_name", obj))) {
          } else if (OB_FAIL(obj.get_string(def.column_name_))) {
          } else if (OB_FAIL(result->get_obj("max_retries", obj))) {
          } else if (OB_FAIL(obj.get_int(def.max_retries_))) {
          } else if (OB_FAIL(result->get_obj("state_column_id", obj))) {
          } else if (OB_FAIL(obj.get_int(def.state_column_id_))) {
          } else if (OB_FAIL(result->get_obj("request_column_id", obj))) {
          } else if (OB_FAIL(obj.get_int(def.request_column_id_))) {
          } else if (OB_FAIL(result->get_obj("result_spec_column_id", obj))) {
          } else if (OB_FAIL(obj.get_int(def.result_spec_column_id_))) {
          } else if (OB_FAIL(result->get_obj("normalized_expr", obj))) {
          } else if (!obj.is_null() && OB_FAIL(obj.get_string(def.normalized_expr_))) {
          } else if (OB_FAIL(result->get_obj("timeout_ms", obj))) {
          } else if (!obj.is_null() && OB_FAIL(obj.get_int(def.timeout_ms_))) {
          } else if (OB_FAIL(result->get_obj("retry_delay_ms", obj))) {
          } else if (!obj.is_null() && OB_FAIL(obj.get_int(def.retry_delay_ms_))) {
          } else if (OB_FAIL(result->get_obj("model_name", obj))) {
          } else if (OB_FAIL(obj.get_string(def.model_name_))) {
          } else if (OB_FAIL(result->get_obj("func_type", obj))) {
          } else if (OB_FAIL(obj.get_int(def.func_type_))) {
          } else if (OB_FAIL(result->get_obj("definition_version", obj))) {
          } else if (OB_FAIL(obj.get_int(def.definition_version_))) {
          } else if (OB_FAIL(result->get_obj("database_name", obj))) {
          } else if (OB_FAIL(obj.get_string(def.database_name_))) {
          } else if (OB_FAIL(result->get_obj("table_name", obj))) {
          } else if (OB_FAIL(obj.get_string(def.table_name_))) {
          } else if (OB_FAIL(result->get_obj("state_column_name", obj))) {
          } else if (OB_FAIL(obj.get_string(def.state_column_name_))) {
          } else if (OB_FAIL(result->get_obj("request_column_name", obj))) {
          } else if (OB_FAIL(obj.get_string(def.request_column_name_))) {
          } else if (OB_FAIL(ob_write_string(alloc, def.column_name_, def.column_name_))) {
          } else if (OB_FAIL(ob_write_string(alloc, def.normalized_expr_, def.normalized_expr_))) {
          } else if (OB_FAIL(ob_write_string(alloc, def.model_name_, def.model_name_))) {
          } else if (OB_FAIL(ob_write_string(alloc, def.database_name_, def.database_name_))) {
          } else if (OB_FAIL(ob_write_string(alloc, def.table_name_, def.table_name_))) {
          } else if (OB_FAIL(ob_write_string(alloc, def.state_column_name_, def.state_column_name_))) {
          } else if (OB_FAIL(ob_write_string(alloc, def.request_column_name_, def.request_column_name_))) {
          } else {
            def.column_id_ = column_id;
          }
        }
      }
    }
  }
  return ret;
}

int AiColumnWorker::call_model(ObIAllocator &alloc, const AiColumnDef &def,
                               const ObString &prompt, ObString &result)
{
  int ret = OB_SUCCESS;
  ObAIFuncExprInfo *ai_fun_info = nullptr;
  omt::ObAiServiceGuard ai_service_guard;
  omt::ObAiService *ai_service = ::oceanbase::share::server_service<::oceanbase::omt::ObAiService>();
  const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
  // The result-type hint is recovered from normalized_expr, where the DDL
  // stored it; it becomes the system prompt (and the JSON response format).
  const bool is_json_hint = !def.normalized_expr_.empty()
      && nullptr != strstr(def.normalized_expr_.ptr(), "valid JSON value");
  const bool is_int_hint = !def.normalized_expr_.empty()
      && nullptr != strstr(def.normalized_expr_.ptr(), "base-10 integer");
  ObString system_prompt;
  if (is_json_hint) {
    system_prompt = ObString::make_string(AI_COLUMN_JSON_HINT);
  } else if (is_int_hint) {
    system_prompt = ObString::make_string(AI_COLUMN_INT_HINT);
  }
  if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(alloc, def.model_name_, ai_fun_info))) {
  } else if (OB_ISNULL(ai_service)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ai_service->get_ai_service_guard(ai_service_guard))) {
  } else if (OB_FAIL(ai_service_guard.get_ai_endpoint_by_ai_model_name(def.model_name_, endpoint_info))) {
  } else if (OB_ISNULL(ai_fun_info) || OB_ISNULL(endpoint_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObAIFuncModel model(alloc, *ai_fun_info, *endpoint_info);
    ObJsonObject *config = nullptr;
    ObString prompt_copy = prompt;
    ObString call_result;
    if (def.timeout_ms_ > 0) {
      // At least one second; the client clamps to [1, INT_MAX/1000] anyway.
      model.set_timeout_sec(std::max(static_cast<int64_t>(1), def.timeout_ms_ / 1000));
    }
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(alloc, config))) {
    } else if (is_json_hint
               && OB_FAIL(ObAIFuncUtils::set_json_format_config(
                      alloc, endpoint_info->get_provider(), config))) {
    } else if (0 == def.func_type_) {
      if (OB_FAIL(model.call_completion_with_system(system_prompt, prompt_copy, config,
                                                   call_result))) {
      }
    } else if (1 == def.func_type_) {
      if (OB_FAIL(model.call_dense_embedding(prompt_copy, config, call_result))) {
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCC(ret)) {
      if (call_result.empty()) {
        ret = OB_INVALID_DATA;
      } else if (OB_FAIL(ob_write_string(alloc, call_result, result))) {
      }
    }
  }
  return ret;
}

int AiColumnWorker::publish_group(ObMySQLTransaction &trans,
                                  const ObSEArray<AiColumnTaskRow, 128> &tasks,
                                  const AiColumnTaskGroup &group,
                                  const ObSEArray<AiColumnDef, 4> &defs,
                                  const ObSEArray<ObString, 4> &results,
                                  ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < group.task_indexes_.count(); ++i) {
    const AiColumnTaskRow &task = tasks.at(group.task_indexes_.at(i));
    const AiColumnDef &def = defs.at(i);
    const ObString &result = results.at(i);
    ObString escaped;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(escape_sql_literal(alloc, result, escaped))) {
    } else if (OB_FAIL(sql.assign_fmt(
            "UPDATE `%.*s`.`%.*s` SET `%.*s` = '%.*s', `%.*s` = 'READY', "
            "`%.*s` = %ld WHERE %.*s AND `%.*s` = %ld",
            static_cast<int>(def.database_name_.length()), def.database_name_.ptr(),
            static_cast<int>(def.table_name_.length()), def.table_name_.ptr(),
            static_cast<int>(def.column_name_.length()), def.column_name_.ptr(),
            static_cast<int>(escaped.length()), escaped.ptr(),
            static_cast<int>(def.state_column_name_.length()), def.state_column_name_.ptr(),
            static_cast<int>(def.request_column_name_.length()), def.request_column_name_.ptr(),
            task.request_id_,
            static_cast<int>(task.rowkey_predicate_.length()), task.rowkey_predicate_.ptr(),
            static_cast<int>(def.request_column_name_.length()), def.request_column_name_.ptr(),
            task.request_id_))) {
    } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
    } else {
      if (0 == affected_rows) {
        // Stale: the row was deleted or superseded by a newer request
        // (its request column no longer matches). Do not publish; the
        // task row is still removed below.
        LOG_WARN("ai column publish skipped stale result", K(task.task_id_),
                 K(task.table_id_), K(task.column_id_), K(task.request_id_));
      }
      if (OB_FAIL(sql.assign_fmt(
              "DELETE FROM oceanbase.__all_ai_column_task WHERE task_id = %ld",
              task.task_id_))) {
      } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      }
    }
  }
  return ret;
}

int AiColumnWorker::fail_group(ObMySQLTransaction &trans,
                               const ObSEArray<AiColumnTaskRow, 128> &tasks,
                               const AiColumnTaskGroup &group,
                               const ObSEArray<AiColumnDef, 4> &defs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < group.task_indexes_.count(); ++i) {
    const AiColumnTaskRow &task = tasks.at(group.task_indexes_.at(i));
    const AiColumnDef &def = defs.at(i);
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (task.retry_count_ < def.max_retries_) {
      // Retryable: keep the task, wait retry_delay_ms before it is due.
      if (OB_FAIL(sql.assign_fmt(
              "UPDATE oceanbase.__all_ai_column_task SET state = %ld, "
              "retry_count = %ld, eligible_ts = DATE_ADD(now(6), INTERVAL %ld MILLISECOND) "
              "WHERE task_id = %ld",
              AI_TASK_STATE_RETRY_WAIT, task.retry_count_ + 1, def.retry_delay_ms_,
              task.task_id_))) {
      } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      }
    } else {
      // Retries exhausted: the row's AI column is terminally FAILED and
      // the task row is removed. The state update is guarded by the
      // request id so a superseded row is not marked.
      if (OB_FAIL(sql.assign_fmt(
              "UPDATE `%.*s`.`%.*s` SET `%.*s` = 'FAILED' WHERE %.*s AND `%.*s` = %ld",
              static_cast<int>(def.database_name_.length()), def.database_name_.ptr(),
              static_cast<int>(def.table_name_.length()), def.table_name_.ptr(),
              static_cast<int>(def.state_column_name_.length()), def.state_column_name_.ptr(),
              static_cast<int>(task.rowkey_predicate_.length()), task.rowkey_predicate_.ptr(),
              static_cast<int>(def.request_column_name_.length()), def.request_column_name_.ptr(),
              task.request_id_))) {
      } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      } else if (OB_FAIL(sql.assign_fmt(
                     "DELETE FROM oceanbase.__all_ai_column_task WHERE task_id = %ld",
                     task.task_id_))) {
      } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      }
    }
  }
  return ret;
}

int AiColumnWorker::process_group(ObIAllocator &alloc,
                                  const ObSEArray<AiColumnTaskRow, 128> &tasks,
                                  const AiColumnTaskGroup &group)
{
  int ret = OB_SUCCESS;
  ObSEArray<AiColumnDef, 4> defs;
  ObSEArray<ObString, 4> results;
  ObSEArray<int64_t, 4> live_indexes;
  // Load the AI column definition for every task in the group. A missing
  // definition (dropped column/table) or a definition-version mismatch
  // drops that task without calling the model.
  for (int64_t i = 0; OB_SUCC(ret) && i < group.task_indexes_.count(); ++i) {
    const AiColumnTaskRow &task = tasks.at(group.task_indexes_.at(i));
    AiColumnDef def;
    int load_ret = read_ai_column_def(alloc, task.table_id_, task.column_id_, def);
    if (OB_ENTRY_NOT_EXIST == load_ret) {
      ObMySQLTransaction trans;
      int64_t affected_rows = 0;
      ObSqlString sql;
      if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
      } else if (OB_FAIL(sql.assign_fmt(
                     "DELETE FROM oceanbase.__all_ai_column_task WHERE task_id = %ld",
                     task.task_id_))) {
      } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      }
      if (trans.is_started()) {
        int tmp_ret = trans.end(OB_SUCC(ret));
        if (OB_FAIL(tmp_ret)) {
          LOG_WARN("failed to end trans dropping stale ai task", K(tmp_ret),
                   K(task.task_id_));
        }
      }
      LOG_WARN("ai column definition gone, task dropped", K(task.task_id_),
               K(task.table_id_), K(task.column_id_));
    } else if (OB_FAIL(load_ret)) {
      ret = load_ret;
    } else if (task.definition_version_ != def.definition_version_) {
      ObMySQLTransaction trans;
      int64_t affected_rows = 0;
      ObSqlString sql;
      if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
      } else if (OB_FAIL(sql.assign_fmt(
                     "DELETE FROM oceanbase.__all_ai_column_task WHERE task_id = %ld",
                     task.task_id_))) {
      } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      }
      if (trans.is_started()) {
        int tmp_ret = trans.end(OB_SUCC(ret));
        if (OB_FAIL(tmp_ret)) {
          LOG_WARN("failed to end trans dropping stale ai task", K(tmp_ret),
                   K(task.task_id_));
        }
      }
      LOG_WARN("ai column definition version mismatch, task dropped", K(task.task_id_),
               K(task.definition_version_), K(def.definition_version_));
    } else if (OB_FAIL(defs.push_back(def))) {
    } else if (OB_FAIL(live_indexes.push_back(group.task_indexes_.at(i)))) {
    }
  }
  if (OB_FAIL(ret) || live_indexes.empty()) {
    // Nothing to call: defs could not be loaded (retried next tick), or
    // every task in the group was dropped above.
    return ret;
  }
  // Call the model for every live column. A group publishes as a whole:
  // if any column fails, none publishes and every task is retried (or
  // marked FAILED when retries are exhausted).
  bool all_ok = true;
  for (int64_t i = 0; i < live_indexes.count(); ++i) {
    const AiColumnTaskRow &task = tasks.at(live_indexes.at(i));
    const AiColumnDef &def = defs.at(i);
    ObString result;
    if (OB_FAIL(call_model(alloc, def, task.prompt_, result))) {
      LOG_WARN("ai column model call failed", K(ret), K(task.task_id_),
               K(task.table_id_), K(task.column_id_), K(task.request_id_));
      all_ok = false;
      ret = OB_SUCCESS;
      break;
    } else if (OB_FAIL(results.push_back(result))) {
      all_ok = false;
      break;
    }
  }
  // Rebuild a group view over the live tasks for publish/fail.
  AiColumnTaskGroup live_group;
  live_group.table_id_ = group.table_id_;
  live_group.request_id_ = group.request_id_;
  for (int64_t i = 0; i < live_indexes.count(); ++i) {
    if (OB_FAIL(live_group.task_indexes_.push_back(live_indexes.at(i)))) {
      break;
    }
  }
  if (OB_SUCC(ret) && !live_group.task_indexes_.empty()) {
    ObMySQLTransaction trans;
    int trans_ret = OB_SUCCESS;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_))) {
    } else if (all_ok) {
      trans_ret = publish_group(trans, tasks, live_group, defs, results, alloc);
    } else {
      trans_ret = fail_group(trans, tasks, live_group, defs);
    }
    if (trans.is_started()) {
      const bool commit = OB_SUCC(trans_ret);
      int tmp_ret = trans.end(commit);
      if (OB_FAIL(tmp_ret)) {
        LOG_WARN("failed to end ai column publish trans", K(tmp_ret),
                 K(group.request_id_));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_FAIL(trans_ret)) {
        ret = trans_ret;
      }
    } else if (OB_FAIL(trans_ret)) {
      ret = trans_ret;
    }
  }
  return ret;
}

int AiColumnWorker::run_once()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObArenaAllocator alloc(ObModIds::OB_MODULE_PAGE_ALLOCATOR, OB_MALLOC_NORMAL_BLOCK_SIZE);
    ObSEArray<AiColumnTaskRow, 128> tasks;
    if (OB_FAIL(read_due_tasks(alloc, tasks))) {
      LOG_WARN("ai column worker failed to read due tasks", K(ret));
    } else if (!tasks.empty()) {
      // Group by (table_id, request_id): one row change may own several
      // AI columns, and they publish together.
      ObSEArray<AiColumnTaskGroup, 8> groups;
      for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
        const AiColumnTaskRow &task = tasks.at(i);
        int64_t group_idx = -1;
        for (int64_t j = 0; j < groups.count(); ++j) {
          if (groups.at(j).table_id_ == task.table_id_
              && groups.at(j).request_id_ == task.request_id_) {
            group_idx = j;
            break;
          }
        }
        if (group_idx < 0) {
          AiColumnTaskGroup group;
          group.table_id_ = task.table_id_;
          group.request_id_ = task.request_id_;
          if (OB_FAIL(group.task_indexes_.push_back(i))) {
          } else if (OB_FAIL(groups.push_back(group))) {
          }
        } else if (OB_FAIL(groups.at(group_idx).task_indexes_.push_back(i))) {
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < groups.count(); ++i) {
        if (OB_FAIL(process_group(alloc, tasks, groups.at(i)))) {
          // One bad group must not block the rest of the tick.
          LOG_WARN("ai column worker group failed", K(ret), K(i));
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
