#include "task_worker.h"

TaskWorker* TaskWorker::_task_workers(0);
uint32_t TaskWorker::_num_workers(0);
std::vector<std::thread> TaskWorker::_worker_threads;

