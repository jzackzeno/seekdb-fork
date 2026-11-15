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

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{

// AI COLUMN definition parsed from DDL.
// func_type_: 0 = AI_COMPLETE (completion), 1 = AI_EMBED (dense embedding).
// The resolver fills this from the CREATE/ALTER TABLE parse tree; the DDL
// executor persists it into oceanbase.__all_ai_column. The ObString members
// point into the resolver allocator, which outlives statement execution.
struct AiColumnDef
{
  AiColumnDef()
    : func_type_(0), embed_dim_(-1), max_retries_(3), timeout_ms_(30000),
      retry_delay_ms_(1000), is_ai_column_(false),
      column_id_(common::OB_INVALID_ID),
      state_column_id_(common::OB_INVALID_ID),
      request_column_id_(common::OB_INVALID_ID),
      result_spec_column_id_(common::OB_INVALID_ID)
  {}
  void reset()
  {
    func_type_ = 0;
    embed_dim_ = -1;
    max_retries_ = 3;
    timeout_ms_ = 30000;
    retry_delay_ms_ = 1000;
    is_ai_column_ = false;
    column_id_ = common::OB_INVALID_ID;
    state_column_id_ = common::OB_INVALID_ID;
    request_column_id_ = common::OB_INVALID_ID;
    result_spec_column_id_ = common::OB_INVALID_ID;
    column_name_.reset();
    model_name_.reset();
    prompt_expr_.reset();
    normalized_expr_.reset();
    result_hint_.reset();
    sync_mode_.reset();
    depends_on_.reset();
    dependency_column_ids_.reset();
  }
  bool is_ai_column_;
  int64_t func_type_;
  // Name of the AI column itself, filled in by the table resolver.
  common::ObString column_name_;
  common::ObString model_name_;
  // SQL text of the prompt/content argument of the AI function call.
  common::ObString prompt_expr_;
  // Full AI function call text plus the result-type hint, persisted for the
  // worker and for SHOW CREATE TABLE.
  common::ObString normalized_expr_;
  // Result-type hint only (e.g. "Return exactly one valid JSON value"),
  // empty when the result type needs no hint.
  common::ObString result_hint_;
  common::ObString sync_mode_;
  // Embedding dimension given as the 3rd AI_EMBED argument, -1 if absent.
  int64_t embed_dim_;
  int64_t max_retries_;
  int64_t timeout_ms_;
  int64_t retry_delay_ms_;
  // Column names referenced by the prompt expression.
  common::ObSEArray<common::ObString, 8> depends_on_;
  // Comma-separated column ids of depends_on_, resolved against the table
  // schema once it is complete (CREATE fills this in a post-loop fix-up,
  // ALTER fills it right after add_column). Persisted into
  // oceanbase.__all_ai_column so the DML hook can test whether an UPDATE
  // touched a dependency column without re-parsing the prompt.
  common::ObString dependency_column_ids_;
  // Column id of the AI column itself, filled in by the table resolver
  // after the column id is allocated.
  int64_t column_id_;
  // Column ids of the three hidden columns, filled in by the table
  // resolver after they are built.
  int64_t state_column_id_;
  int64_t request_column_id_;
  int64_t result_spec_column_id_;
};

} // namespace sql
} // namespace oceanbase
