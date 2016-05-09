#include "task_worker.h"

template <typename Resolution>
class Timer {
   uint64_t _start;

  public:
   uint64_t now() {
      return std::chrono::duration_cast<Resolution>(
                 std::chrono::high_resolution_clock::now().time_since_epoch())
          .count();
   }

   void start() { _start = now(); }

   uint64_t stop() { return now() - _start; }
};

uint32_t g_work = 10;
uint32_t g_thread_task_count[8] = {0};

void empty_job(Task*, void*, int thread_id) {
   // do nothing just count which thread we executed on
   g_thread_task_count[thread_id]++;
   usleep(g_work);
}

int main(int argc, char** argv) {
   // std::cout << "sizeof(Task): " << sizeof(Task) << std::endl;
   // std::cout << "alignof(Task): " << alignof(Task) << std::endl;
   // std::cout << "sizeof(Task::padding): " << sizeof(Task::padding) <<
   // std::endl;

   TaskWorker::init(4);

   static const int TEST_TASKS = 65000;
   Timer<std::chrono::milliseconds> timer;
   uint32_t total_work = g_work * TEST_TASKS / 1000;
   uint32_t task_count = 0;

   // test parallel job creation
   Task* root = TaskWorker::create_task(&empty_job);
   for (int i = 0; i < TEST_TASKS - 1; ++i) {
      Task* task =
          TaskWorker::create_child_task(root, &empty_job);
      TaskWorker::run(task);
   }
   timer.start();
   TaskWorker::run(root);
   TaskWorker::wait(root);
   uint64_t tm_parallel = timer.stop();
   std::cout << "Parallel jobs took " << tm_parallel << " ms for " << total_work
             << "ms of work (" << float(total_work) / float(tm_parallel) << "x)"
             << std::endl;
   task_count = 0;
   for (uint32_t i = 0; i < TaskWorker::num_workers(); ++i) {
      std::cout << "[" << i << "]: " << g_thread_task_count[i] << std::endl;
      task_count += g_thread_task_count[i];
      g_thread_task_count[i] = 0;
   }
   std::cout << "Total: " << task_count << std::endl;

   TaskWorker::shutdown();

   return 0;
}
