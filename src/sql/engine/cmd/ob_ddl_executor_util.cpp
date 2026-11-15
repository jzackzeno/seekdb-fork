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
#include <atomic>
#include "query/command/ob_local_command_service.h"
#include "share/ob_server_struct.h"
#include "query/command/ob_root_command_service.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "ob_ddl_executor_util.h"
#include "query/engine/cmd/ob_ddl_execution.h"
#include "share/ob_ddl_task_executor.h"
#include "share/ob_ex_rpc.h"
#include "share/ob_structured_event_logger.h"
#include "share/ob_share_util.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_dml_sql_splicer.h"
#include "sql/resolver/ddl/ob_ai_column_def.h"
#include "common/object/ob_object.h"
#include "oblib/common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{
int ObDDLExecutorUtil::handle_session_exception(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  
  bool write_enabled = true;
  if (OB_UNLIKELY(session.is_query_killed())) {
    ret = OB_ERR_QUERY_INTERRUPTED;
  } else if (OB_UNLIKELY(session.is_zombie())) {
    ret = OB_SESSION_KILLED;
  } else if (OB_FAIL(ObShareUtil::is_server_write_enabled(write_enabled))) {
  } else if (!write_enabled) {
    ret = OB_SESSION_KILLED;
  }
  return ret;
}

int ObDDLExecutorUtil::wait_ddl_finish(const int64_t task_id,
    const bool ddl_need_retry_at_executor,
    ObSQLSessionInfo *session,
    query::ObIQueryRuntimeEnvironment &runtime_environment,
    query::ObILocalCommandService &local_command_service,
    const bool is_support_cancel)
{
  int ret = OB_SUCCESS;
  const int64_t retry_interval = 100 * 1000;
  ObAddr unused_addr;
  bool is_table_exist = false;
  int64_t unused_user_msg_len = 0;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
  ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
  if (OB_UNLIKELY(task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    SERVER_EVENT_ADD("ddl", "start wait ddl finish",
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "task_id", task_id,
      "rpc_dest", GCTX.self_addr());
    LOG_INFO("start wait ddl finsih", K(task_id), "ddl_event_info", ObDDLEventInfo(GCTX.self_addr()));

    int tmp_ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      if (OB_SUCCESS == ObDDLErrorMessageTableOperator::get_ddl_error_message(task_id, -1 /* target_object_id */, unused_addr, false /* is_ddl_retry_task */, *GCTX.sql_proxy_, error_message, unused_user_msg_len)) {
        ret = error_message.ret_code_;
        if (OB_SUCCESS != ret) {
          if (ddl_need_retry_at_executor) {
            ret = share::ObIDDLTask::in_ddl_retry_white_list(ret) ? OB_EAGAIN : ret;
          } else {
            FORWARD_USER_ERROR(ret, error_message.user_message_);
          }
        } else if (error_message.published_schema_version_ != OB_INVALID_VERSION) {
          ObTimeoutCtx ctx;
          int64_t start_time = ObTimeUtility::current_time();
          if (OB_FAIL(ctx.set_timeout(THIS_WORKER.get_timeout_remain()))) {
          } else if (OB_FAIL(ObDDLExecutorUtil::wait_local_schema_visible(
                      ctx, session, error_message.published_schema_version_))) {
          } else {
            int64_t refresh_time = ObTimeUtility::current_time() - start_time;
            LOG_INFO("parallel ddl wait schema", KR(ret), K(refresh_time),
                                                 K_(error_message.published_schema_version));
          }
        }
        break;
      } else {
        if (OB_FAIL(ret)) {
        }

        if (OB_FAIL(ret)) {
        } else if (nullptr != session && OB_FAIL(handle_session_exception(*session))) {
          if (is_support_cancel && OB_TMP_FAIL(cancel_ddl_task(local_command_service))) {
            LOG_WARN("cancel ddl task failed", K(tmp_ret));
            ret = OB_SUCCESS;
          } else {
            break;
          }
        } 
        
        if (OB_FAIL(ret)) {
        } else if (is_server_stopped(runtime_environment)) {
          ret = OB_TIMEOUT;
          FORWARD_USER_ERROR(ret, "DDL execution status is undecided, please check later if it finishes successfully or not.");
        } else {
          ob_usleep(retry_interval);
        }
      }
    }

    SERVER_EVENT_ADD("ddl", "end wait ddl finish",
      "ret", error_message.ret_code_,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "task_id", task_id,
      "rpc_dest", GCTX.self_addr());
    LOG_INFO("finish wait ddl", K(ret), K(task_id), "ddl_event_info", ObDDLEventInfo(GCTX.self_addr()), K(error_message));
  }
  return ret;
}

int ObDDLExecutorUtil::wait_build_index_finish(const int64_t task_id, bool &is_finish)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObAddr unused_addr;
  int64_t unused_user_msg_len = 0;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
  share::ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
  is_finish = false;
  SERVER_EVENT_ADD("ddl", "start wait build index finish",
    "ret", ret,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "task_id", task_id);
  LOG_INFO("start wait build index finish", K(task_id), "ddl_event_info", ObDDLEventInfo(GCTX.self_addr()));

  if (OB_UNLIKELY(task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS == share::ObDDLErrorMessageTableOperator::get_ddl_error_message(task_id, -1 /* target_object_id */, unused_addr, false /* is_ddl_retry_task */, *GCTX.sql_proxy_, error_message, unused_user_msg_len)) {
    ret = error_message.ret_code_;
    if (OB_SUCCESS != ret) {
      FORWARD_USER_ERROR(ret, error_message.user_message_);
    }
    is_finish = true;
  }

  SERVER_EVENT_ADD("ddl", "end wait build index finish",
    "ret", error_message.ret_code_,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "task_id", task_id);
  LOG_INFO("finish wait build index", K(ret), "ddl_event_info", ObDDLEventInfo(GCTX.self_addr()), K(error_message));
  return ret;
}

int ObDDLExecutorUtil::wait_ddl_retry_task_finish(const int64_t task_id,
    ObSQLSessionInfo &session,
    query::ObIQueryRuntimeEnvironment &runtime_environment,
    query::ObILocalCommandService &local_command_service,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  const int64_t retry_interval = 100 * 1000;
  ObAddr unused_addr;
  bool is_table_exist = false;
  int64_t forward_user_msg_len = 0;
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + OB_MAX_USER_SPECIFIED_TIMEOUT);
  ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
  if (OB_UNLIKELY(task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    SERVER_EVENT_ADD("ddl", "start wait ddl retry task finish",
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "task_id", task_id,
      "rpc_dest", GCTX.self_addr());
    LOG_INFO("start wait ddl retry task finish", K(task_id), "ddl_event_info", ObDDLEventInfo(GCTX.self_addr()), K(error_message));

    bool write_enabled = true;
    int tmp_ret = OB_SUCCESS;
    while (OB_SUCC(ret)) {
      if (OB_SUCCESS == ObDDLErrorMessageTableOperator::get_ddl_error_message(task_id, -1 /* target_object_id */, unused_addr, true /* is_ddl_retry_task */, *GCTX.sql_proxy_, error_message, forward_user_msg_len)) {
        // Here, `forward_user_msg_len` is the length of serialized hex user message.
        // Forward_user_msg_len is not 0, which means rpc::frame::ObResultCode is not empty. Thus, we need to
        // forward_user_error/ forward_user_warn/ forward_user_note.
        ret = error_message.ret_code_;
        if (OB_UNLIKELY(forward_user_msg_len == 0 && OB_SUCCESS != error_message.ret_code_)) {
          const char *str_user_error = ob_errpkt_strerror(error_message.ret_code_);
          FORWARD_USER_ERROR(error_message.ret_code_, str_user_error);
          FLOG_INFO("error code is not succ, but forward user msg is null", K(ret), K(error_message), K(str_user_error));
        } else if (forward_user_msg_len > 0) {
          int64_t pos = 0;
          int tmp_ret = OB_SUCCESS;
          rpc::frame::ObResultCode result_code;
          if (OB_SUCCESS != (tmp_ret = result_code.deserialize(error_message.user_message_, forward_user_msg_len, pos))) {
          } else if (OB_UNLIKELY(OB_SUCCESS != result_code.rcode_)) {
            FORWARD_USER_ERROR(result_code.rcode_, result_code.msg_);
          } else {
            for (int i = 0; OB_SUCCESS == tmp_ret && i < result_code.warnings_.count(); ++i) {
              const common::ObWarningBuffer::WarningItem warning_item = result_code.warnings_.at(i);
              if (ObLogger::USER_WARN == warning_item.log_level_) {
                FORWARD_USER_WARN(warning_item.code_, warning_item.msg_);
              } else if (ObLogger::USER_NOTE == warning_item.log_level_) {
                FORWARD_USER_NOTE(warning_item.code_, warning_item.msg_);
              } else {
                tmp_ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unknown log type", K(ret), K(tmp_ret), K(warning_item));
              }
            }
          }
        }
        break;
        } else {
          if (OB_FAIL(ret)) {
         } else if (OB_TMP_FAIL(ObShareUtil::is_server_write_enabled(write_enabled))) {
         } else if (!write_enabled) {
          ret = OB_STANDBY_DATABASE_READ_ONLY;
          FORWARD_USER_ERROR(ret, "DDL execution status is undecided, please check later if it finishes successfully or not.");
          break;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(handle_session_exception(session))) {
          if (OB_TMP_FAIL(cancel_ddl_task(local_command_service))) {
            LOG_WARN("cancel ddl task failed", K(tmp_ret));
            ret = OB_SUCCESS;
          } else {
            break;
          }
        } 
        
        if (OB_FAIL(ret)) {
        } else if (is_server_stopped(runtime_environment)) {
          ret = OB_TIMEOUT;
          FORWARD_USER_ERROR(ret, "DDL execution status is undecided, please check later if it finishes successfully or not.");
        } else {
          ob_usleep(retry_interval);
        }
      }
    }
    affected_rows = error_message.affected_rows_;

    SERVER_EVENT_ADD("ddl", "end wait ddl retry task finish",
      "ret", error_message.ret_code_,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "task_id", task_id,
      "rpc_dest", GCTX.self_addr());
    LOG_INFO("fnish wait ddl retry task", K(ret), K(task_id), "ddl_event_info", ObDDLEventInfo(GCTX.self_addr()), K(error_message));
  }
  return ret;
}

int ObDDLExecutorUtil::cancel_ddl_task(
    query::ObILocalCommandService &local_command_service)
{
  int ret = OB_SUCCESS;
  obcall::ObCancelTaskArg rpc_arg;
  rpc_arg.task_id_ = *ObCurTraceId::get_trace_id();

  if (OB_FAIL(ex_rpc::sync_call(
      [&]{ return local_command_service.cancel_sys_task(rpc_arg.task_id_); }))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
    }
  }
  SERVER_EVENT_ADD("ddl", "finish cancel ddl task",
    "ret", ret,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "rpc_dest", GCTX.self_addr());
  LOG_INFO("finish cancel ddl task", K(ret), K(rpc_arg), "rpc_dest", GCTX.self_addr(), "ddl_event_info", ObDDLEventInfo(GCTX.self_addr()));
  return ret;
}

int ObDDLExecutorUtil::execute_pcreate_table(ObSQLSessionInfo *my_session,
                                            query::ObIRootCommandService &root_commands,
                                            const char* parallel_ddl_type,
                                            const obcall::ObCreateTableArg &arg, obcall::ObCreateTableRes &res)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  ObTimeoutCtx ctx;
  if (OB_FAIL(ctx.set_timeout(THIS_WORKER.get_timeout_remain()))) {
  } else if (OB_FAIL(root_commands.parallel_create_table(arg, res))) {
  } else {
    int64_t refresh_time = ObTimeUtility::current_time();
    if (!res.do_nothing_ && OB_FAIL(ObDDLExecutorUtil::wait_local_schema_visible(
        ctx, my_session, res.schema_version_))) {
    }
    int64_t end_time = ObTimeUtility::current_time();
    LOG_INFO(parallel_ddl_type, KR(ret),
            "cost", end_time - start_time,
            "execute_time", refresh_time - start_time,
            "wait_schema", end_time - refresh_time);
  }
  return ret;
}

bool ObDDLExecutorUtil::is_server_stopped(
    query::ObIQueryRuntimeEnvironment &runtime_environment)
{
  return query::query_server_stopped(runtime_environment);
}

} //end namespace sql
} //end namespace oceanbase

namespace oceanbase
{
namespace sql
{
int ObDDLExecutorUtil::wait_local_schema_visible(
    const ObTimeoutCtx &ctx,
    sql::ObSQLSessionInfo *session,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = NULL;
  bool schema_visible = false;
  if (OB_ISNULL(session) || OB_UNLIKELY(schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(schema_service = GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
  }
  while (OB_SUCC(ret) && ctx.get_timeout() > 0 && !schema_visible) {
    int64_t refreshed_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(ObDDLExecutorUtil::handle_session_exception(*session))) {
    } else if (OB_FAIL(schema_service->get_runtime_refreshed_schema_version(refreshed_schema_version))) {
    } else if (refreshed_schema_version >= schema_version) {
      schema_visible = true;
    } else {
      if (REACH_TIME_INTERVAL(1000 * 1000L)) { // 1s
        LOG_WARN("local schema version not visible", K(refreshed_schema_version), K(schema_version));
      }
      ob_usleep(10 * 1000L); // 10ms
    }
  }
  if (OB_SUCC(ret) && !schema_visible) {
    ret = OB_TIMEOUT;
  }
  return ret;
}

// Build the name of one hidden column of an AI column. Must stay in sync
// with ObDDLResolver::build_ai_column_hidden_columns (including the
// truncation for very long AI column names).
static int build_ai_hidden_column_name(const ObString &ai_column_name,
                                       const char *suffix,
                                       ObSqlString &name_buf)
{
  int ret = OB_SUCCESS;
  name_buf.reset();
  if (OB_FAIL(name_buf.append("__ai_"))) {
  } else if (OB_FAIL(name_buf.append(ai_column_name))) {
  } else if (OB_FAIL(name_buf.append(suffix))) {
  }
  if (OB_SUCC(ret) && name_buf.string().length() > OB_MAX_COLUMN_NAME_LENGTH) {
    const int64_t suffix_len = static_cast<int64_t>(strlen(suffix));
    const int64_t keep = OB_MAX_COLUMN_NAME_LENGTH - 5 - suffix_len;
    name_buf.reset();
    if (OB_FAIL(name_buf.append("__ai_"))) {
    } else if (OB_FAIL(name_buf.append(ai_column_name.ptr(), keep))) {
    } else if (OB_FAIL(name_buf.append(suffix))) {
    }
  }
  return ret;
}

int ObDDLExecutorUtil::write_ai_column_meta(ObMySQLProxy &sql_proxy,
                                            const uint64_t table_id,
                                            const ObIArray<AiColumnDef> &ai_defs,
                                            const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ai_defs.count(); ++i) {
    const AiColumnDef &def = ai_defs.at(i);
    int64_t column_id = def.column_id_;
    int64_t state_column_id = def.state_column_id_;
    int64_t request_column_id = def.request_column_id_;
    int64_t result_spec_column_id = def.result_spec_column_id_;
    if (NULL != table_schema) {
      // ALTER path: rootserver assigned the column ids; resolve them from
      // the refreshed schema by column name.
      const ObColumnSchemaV2 *col = table_schema->get_column_schema(def.column_name_);
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("AI column not found in refreshed schema", K(table_id), K(def.column_name_));
      } else {
        column_id = col->get_column_id();
        const char *suffixes[3] = {"__state", "__request", "__result_spec"};
        int64_t *ids[3] = {&state_column_id, &request_column_id, &result_spec_column_id};
        for (int64_t k = 0; OB_SUCC(ret) && k < 3; ++k) {
          ObSqlString name_buf;
          if (OB_FAIL(build_ai_hidden_column_name(def.column_name_, suffixes[k], name_buf))) {
          } else if (OB_ISNULL(col = table_schema->get_column_schema(name_buf.string()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("AI hidden column not found in refreshed schema", K(table_id),
                     K(def.column_name_), K(suffixes[k]));
          } else {
            *ids[k] = col->get_column_id();
          }
        }
      }
    }
    if (OB_SUCC(ret)
        && (OB_INVALID_ID == column_id || OB_INVALID_ID == state_column_id
            || OB_INVALID_ID == request_column_id
            || OB_INVALID_ID == result_spec_column_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("AI column metadata has invalid column id", K(table_id), K(def.column_name_),
               K(column_id), K(state_column_id), K(request_column_id), K(result_spec_column_id));
    }
    if (OB_SUCC(ret)) {
      ObDMLSqlSplicer dml_splicer;
      ObSqlString insert_sql;
      const ObString func_type_str = (0 == def.func_type_)
          ? ObString::make_string("COMPLETION")
          : ObString::make_string("EMBEDDING");
      if (OB_FAIL(dml_splicer.add_pk_column("table_id", static_cast<int64_t>(table_id)))) {
      } else if (OB_FAIL(dml_splicer.add_pk_column("column_id", column_id))) {
      } else if (OB_FAIL(dml_splicer.add_column("column_name", ObHexEscapeSqlStr(def.column_name_)))) {
      } else if (OB_FAIL(dml_splicer.add_column("max_retries", def.max_retries_))) {
      } else if (OB_FAIL(dml_splicer.add_column("state_column_id", state_column_id))) {
      } else if (OB_FAIL(dml_splicer.add_column("request_column_id", request_column_id))) {
      } else if (OB_FAIL(dml_splicer.add_column("result_spec_column_id", result_spec_column_id))) {
      } else if (OB_FAIL(dml_splicer.add_column("normalized_expr", ObHexEscapeSqlStr(def.normalized_expr_)))) {
      } else if (OB_FAIL(dml_splicer.add_column("sync_mode", ObHexEscapeSqlStr(def.sync_mode_)))) {
      } else if (OB_FAIL(dml_splicer.add_column("timeout_ms", def.timeout_ms_))) {
      } else if (OB_FAIL(dml_splicer.add_column("retry_delay_ms", def.retry_delay_ms_))) {
      } else if (OB_FAIL(dml_splicer.add_column("model_name", ObHexEscapeSqlStr(def.model_name_)))) {
      } else if (OB_FAIL(dml_splicer.add_column("func_type", ObHexEscapeSqlStr(func_type_str)))) {
      } else if (OB_FAIL(dml_splicer.add_column("prompt_expr", ObHexEscapeSqlStr(def.prompt_expr_)))) {
      } else if (OB_FAIL(dml_splicer.add_column("definition_version", static_cast<int64_t>(1)))) {
      } else if (OB_FAIL(dml_splicer.add_column("dependency_column_ids", ObHexEscapeSqlStr(def.dependency_column_ids_)))) {
      } else if (OB_FAIL(dml_splicer.splice_insert_sql("oceanbase.__all_ai_column", insert_sql))) {
      }
      int64_t affected_rows = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql_proxy.write(insert_sql.ptr(), affected_rows))) {
        LOG_WARN("fail to insert __all_ai_column", K(ret), K(table_id), K(column_id));
      } else if (OB_FAIL(sql_proxy.write("commit;", affected_rows))) {
        LOG_WARN("fail to commit __all_ai_column insert", K(ret));
      }
    }
  }
  return ret;
}

// Monotonic id generator for RETRY-created AI column tasks. Seeded from
// the current time in microseconds, like the DML hook's generator in
// ob_dml_service.cpp; the two counters are seeded at different times and
// advance far slower than wall-clock microseconds, so ids do not collide.
static std::atomic<int64_t> g_retry_ai_id_seq{0};
static int64_t next_retry_ai_id()
{
  int64_t seq = g_retry_ai_id_seq.load();
  if (0 == seq) {
    const int64_t now = ObTimeUtility::current_time();
    if (g_retry_ai_id_seq.compare_exchange_strong(seq, now)) {
      return now;
    }
    seq = g_retry_ai_id_seq.load();
  }
  return g_retry_ai_id_seq.fetch_add(1);
}

// Append a SQL string literal for value (single quotes doubled).
static int append_escaped_sql_string(ObSqlString &out, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(out.append("'"))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
      if ('\'' == value.ptr()[i] && OB_FAIL(out.append("''"))) {
      } else if (OB_FAIL(out.append(value.ptr() + i, 1))) {
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(out.append("'"))) {
    }
  }
  return ret;
}

int ObDDLExecutorUtil::retry_ai_column(ObMySQLProxy &sql_proxy,
                                       const uint64_t table_id,
                                       const ObString &ai_column_name,
                                       const ObString &where_sql)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc(ObModIds::OB_SQL_EXECUTOR);
  // 1. Read the AI column definition and the database/table names.
  int64_t column_id = OB_INVALID_ID;
  int64_t state_column_id = OB_INVALID_ID;
  int64_t request_column_id = OB_INVALID_ID;
  int64_t definition_version = 0;
  ObString prompt_expr;
  ObString state_col_name;
  ObString request_col_name;
  ObString db_name;
  ObString table_name;
  {
    ObSqlString def_sql;
    ObSqlString escaped_name;
    if (OB_FAIL(append_escaped_sql_string(escaped_name, ai_column_name))
        || OB_FAIL(def_sql.assign_fmt(
               "SELECT c.column_id, c.state_column_id, c.request_column_id, "
               "c.prompt_expr, c.definition_version, "
               "hs.column_name AS state_col, hr.column_name AS request_col, "
               "d.database_name, t.table_name "
               "FROM oceanbase.__all_ai_column c "
               "JOIN oceanbase.__all_column hs "
               "  ON hs.table_id = c.table_id AND hs.column_id = c.state_column_id "
               "JOIN oceanbase.__all_column hr "
               "  ON hr.table_id = c.table_id AND hr.column_id = c.request_column_id "
               "JOIN oceanbase.__all_table t ON t.table_id = c.table_id "
               "JOIN oceanbase.__all_database d ON d.database_id = t.database_id "
               "WHERE c.table_id = %ld AND c.column_name = %.*s",
               static_cast<int64_t>(table_id),
               static_cast<int>(escaped_name.string().length()),
               escaped_name.string().ptr()))) {
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy.read(res, def_sql.ptr()))) {
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            // Resolver validated the target; a missing row means the
            // metadata was dropped concurrently.
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("AI column metadata not found for retry", K(table_id), K(ai_column_name));
          }
        } else {
          ObObj obj;
          if (OB_FAIL(result->get_obj("column_id", obj))) {
          } else if (OB_FAIL(obj.get_int(column_id))) {
          } else if (OB_FAIL(result->get_obj("state_column_id", obj))) {
          } else if (OB_FAIL(obj.get_int(state_column_id))) {
          } else if (OB_FAIL(result->get_obj("request_column_id", obj))) {
          } else if (OB_FAIL(obj.get_int(request_column_id))) {
          } else if (OB_FAIL(result->get_obj("definition_version", obj))) {
          } else if (OB_FAIL(obj.get_int(definition_version))) {
          } else if (OB_FAIL(result->get_obj("prompt_expr", obj))) {
          } else if (OB_FAIL(obj.get_string(prompt_expr))) {
          } else if (OB_FAIL(result->get_obj("state_col", obj))) {
          } else if (OB_FAIL(obj.get_string(state_col_name))) {
          } else if (OB_FAIL(result->get_obj("request_col", obj))) {
          } else if (OB_FAIL(obj.get_string(request_col_name))) {
          } else if (OB_FAIL(result->get_obj("database_name", obj))) {
          } else if (OB_FAIL(obj.get_string(db_name))) {
          } else if (OB_FAIL(result->get_obj("table_name", obj))) {
          } else if (OB_FAIL(obj.get_string(table_name))) {
          } else if (OB_FAIL(ob_write_string(alloc, prompt_expr, prompt_expr))) {
          } else if (OB_FAIL(ob_write_string(alloc, state_col_name, state_col_name))) {
          } else if (OB_FAIL(ob_write_string(alloc, request_col_name, request_col_name))) {
          } else if (OB_FAIL(ob_write_string(alloc, db_name, db_name))) {
          } else if (OB_FAIL(ob_write_string(alloc, table_name, table_name))) {
          }
        }
      }
    }
  }
  // 2. Read the primary key columns (RETRY needs a rowkey predicate).
  ObSEArray<ObString, 4> rowkey_columns;
  if (OB_SUCC(ret)) {
    ObSqlString pk_sql;
    if (OB_FAIL(pk_sql.assign_fmt(
            "SELECT column_name FROM oceanbase.__all_column "
            "WHERE table_id = %ld AND rowkey_position > 0 ORDER BY rowkey_position",
            static_cast<int64_t>(table_id)))) {
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy.read(res, pk_sql.ptr()))) {
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
              ObObj obj;
              ObString col_name;
              if (OB_FAIL(result->get_obj("column_name", obj))) {
              } else if (OB_FAIL(obj.get_string(col_name))) {
              } else if (OB_FAIL(ob_write_string(alloc, col_name, col_name))) {
              } else if (OB_FAIL(rowkey_columns.push_back(col_name))) {
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && rowkey_columns.empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "retry ai column on table without primary key");
    }
  }
  // 3. Select the primary key values of the matching rows.
  ObSEArray<ObString, 16> rowkey_predicates;
  if (OB_SUCC(ret)) {
    ObSqlString select_sql;
    if (OB_FAIL(select_sql.append("SELECT "))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_columns.count(); ++i) {
        if (i > 0 && OB_FAIL(select_sql.append(", "))) {
        } else if (OB_FAIL(select_sql.append("`"))
                   || OB_FAIL(select_sql.append(rowkey_columns.at(i)))
                   || OB_FAIL(select_sql.append("`"))) {
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(select_sql.append(" FROM `"))) {
      } else if (OB_SUCC(ret) && OB_FAIL(select_sql.append(db_name))) {
      } else if (OB_SUCC(ret) && OB_FAIL(select_sql.append("`.`"))) {
      } else if (OB_SUCC(ret) && OB_FAIL(select_sql.append(table_name))) {
      } else if (OB_SUCC(ret) && OB_FAIL(select_sql.append("`"))) {
      } else if (OB_SUCC(ret) && !where_sql.empty()
                 && (OB_FAIL(select_sql.append(" WHERE "))
                     || OB_FAIL(select_sql.append(where_sql)))) {
      }
    }
    if (OB_SUCC(ret)) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy.read(res, select_sql.ptr()))) {
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
              ObSqlString predicate;
              for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_columns.count(); ++i) {
                ObObj obj;
                // Read by column index: the SELECT lists the rowkey
                // columns in the same order.
                if (OB_FAIL(result->get_obj(i, obj))) {
                } else if (i > 0 && OB_FAIL(predicate.append(" AND "))) {
                } else if (OB_FAIL(predicate.append("`"))
                           || OB_FAIL(predicate.append(rowkey_columns.at(i)))
                           || OB_FAIL(predicate.append("` = "))) {
                } else if (obj.is_null()) {
                  if (OB_FAIL(predicate.append("NULL"))) {
                  }
                } else if (obj.is_varchar() || obj.is_char()) {
                  ObString str_val;
                  if (OB_FAIL(obj.get_string(str_val))) {
                  } else if (OB_FAIL(append_escaped_sql_string(predicate, str_val))) {
                  }
                } else if (obj.is_int() || obj.is_uint64()) {
                  int64_t int_val = 0;
                  if (OB_FAIL(obj.get_int(int_val))) {
                  } else if (OB_FAIL(predicate.append_fmt("%ld", int_val))) {
                  }
                } else {
                  ret = OB_NOT_SUPPORTED;
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "retry ai column on non-integer primary key");
                }
              }
              if (OB_SUCC(ret)) {
                ObString predicate_copy;
                if (OB_FAIL(ob_write_string(alloc, predicate.string(), predicate_copy))) {
                } else if (OB_FAIL(rowkey_predicates.push_back(predicate_copy))) {
                }
              }
            }
          }
        }
      }
    }
  }
  // 4. Invalidate the AI column and create a task per matching row, in
  // one transaction. Zero matching rows is a legal no-op.
  if (OB_SUCC(ret) && !rowkey_predicates.empty()) {
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(sql_proxy))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_predicates.count(); ++i) {
        const int64_t new_request_id = next_retry_ai_id();
        const int64_t task_id = next_retry_ai_id();
        ObSqlString update_sql;
        ObSqlString task_sql;
        int64_t affected_rows = 0;
        if (OB_FAIL(update_sql.assign_fmt(
                "UPDATE `%.*s`.`%.*s` SET `%.*s` = NULL, `%.*s` = 'PENDING', "
                "`%.*s` = %ld WHERE %.*s",
                static_cast<int>(db_name.length()), db_name.ptr(),
                static_cast<int>(table_name.length()), table_name.ptr(),
                static_cast<int>(ai_column_name.length()), ai_column_name.ptr(),
                static_cast<int>(state_col_name.length()), state_col_name.ptr(),
                static_cast<int>(request_col_name.length()), request_col_name.ptr(),
                new_request_id,
                static_cast<int>(rowkey_predicates.at(i).length()),
                rowkey_predicates.at(i).ptr()))
            || OB_FAIL(trans.write(update_sql.ptr(), affected_rows))) {
        } else if (OB_FAIL(task_sql.assign_fmt(
                       "INSERT INTO oceanbase.__all_ai_column_task "
                       "(task_id, table_id, column_id, request_id, rowkey_predicate, "
                       "state, retry_count, eligible_ts, prompt, definition_version) "
                       "SELECT %ld, %ld, %ld, %ld, '%.*s', 0, 0, now(6), %.*s, %ld "
                       "FROM `%.*s`.`%.*s` WHERE %.*s",
                       task_id, static_cast<int64_t>(table_id), column_id, new_request_id,
                       static_cast<int>(rowkey_predicates.at(i).length()),
                       rowkey_predicates.at(i).ptr(),
                       static_cast<int>(prompt_expr.length()), prompt_expr.ptr(),
                       definition_version,
                       static_cast<int>(db_name.length()), db_name.ptr(),
                       static_cast<int>(table_name.length()), table_name.ptr(),
                       static_cast<int>(rowkey_predicates.at(i).length()),
                       rowkey_predicates.at(i).ptr()))
                   || OB_FAIL(trans.write(task_sql.ptr(), affected_rows))) {
        }
      }
      if (trans.is_started()) {
        int tmp_ret = trans.end(OB_SUCC(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
  return ret;
}
}  // namespace sql

namespace query
{

int ObDDLExecution::wait_ddl_finish(
    const int64_t task_id,
    const bool ddl_need_retry_at_executor,
    sql::ObSQLSessionInfo *session,
    ObIQueryRuntimeEnvironment &runtime_environment,
    ObILocalCommandService &local_command_service,
    const bool is_support_cancel)
{
  return sql::ObDDLExecutorUtil::wait_ddl_finish(
      task_id, ddl_need_retry_at_executor, session,
      runtime_environment, local_command_service, is_support_cancel);
}

int ObDDLExecution::handle_session_exception(sql::ObSQLSessionInfo &session)
{
  return sql::ObDDLExecutorUtil::handle_session_exception(session);
}

} // namespace query
}  // namespace oceanbase
