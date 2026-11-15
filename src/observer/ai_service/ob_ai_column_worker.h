#pragma once

#include <atomic>
#include "lib/task/ob_timer_service.h"

namespace oceanbase
{
namespace observer
{

// Periodic task that drives AI columns to their terminal state. It is owned
// by ObAiService and scheduled on its timer every 200ms. Each tick claims
// due rows of oceanbase.__all_ai_column_task, calls the model for each
// request group, and publishes the results (or retries / marks FAILED) in
// one transaction per group. The run is guarded against overlap: a tick
// that arrives while the previous one is still calling a model is skipped.
class AiColumnWorkerTask : public common::ObTimerTask
{
public:
  AiColumnWorkerTask();
  ~AiColumnWorkerTask() override;
  void runTimerTask() override;

private:
  std::atomic<bool> running_;
};

} // namespace observer
} // namespace oceanbase
