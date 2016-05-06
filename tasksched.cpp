#include <functional>
#include <atomic>
#include <iostream>
#include <deque>
#include <random>
#include <thread>
#include <cassert>
#include <mutex>
#include <unistd.h>

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

struct Task {
   using Function = std::function<void(Task*, void*, int)>;
   Function function;
   Task* parent;
   std::atomic_int unfinished_tasks;
   uint8_t padding[128 - sizeof(Function) - sizeof(Task*) -
                   sizeof(std::atomic_int)];

   bool has_completed() const { return unfinished_tasks == 0; }
};

uint32_t g_thread_task_count[8] = {0};

struct TaskWorker {
   int32_t index;
   // TaskQueue queue;
   static const uint32_t QUEUE_SIZE = 65536;
   static const uint32_t QUEUE_MASK = QUEUE_SIZE - 1u;
   static const int TASKSCHED_MAX_JOBS = 65536;
   static const int TASKSCHED_ALLOC_SIZE = TASKSCHED_MAX_JOBS;
   Task* _task_queue[QUEUE_SIZE];
   uint32_t _bottom;
   uint32_t _top;
   std::atomic_bool active;
   std::random_device rd;
   std::default_random_engine re;
   std::uniform_int_distribution<int32_t> udist;
   std::mutex mtx_q;
   Task* _task_allocator;
   int _tasks_allocated;

   static uint32_t _num_workers;
   static TaskWorker* _task_workers;

   TaskWorker() :  _bottom(0), _top(0), udist(0, _num_workers - 1) {
      _tasks_allocated = 0;
      _task_allocator = new Task[TASKSCHED_ALLOC_SIZE];
   }

   ~TaskWorker() { delete[] _task_allocator; }

   Task* create_task(Task::Function function) {
      Task* task = allocate_task();
      task->function = function;
      task->parent = nullptr;
      task->unfinished_tasks = 1;
      return task;
   }

   Task* create_child_task(Task* parent, Task::Function function) {
      parent->unfinished_tasks++;
      Task* task = allocate_task();
      task->function = function;
      task->parent = parent;
      task->unfinished_tasks = 1;
      return task;
   }

   void run(Task* task) { push(task); }

   void wait(const Task* task) {
      while (!task->has_completed()) {
         Task* next_task = get_task();
         if (next_task) {
            execute(next_task);
         }
      }
   }

   static void init(uint32_t num_threads = 0) {
      // Set the number of workers to the number of threads passed by the user.
      // If they select 0 then set it equal to the number of logical cores.
      if (num_threads > 0) {
         _num_workers = num_threads;
      } else {
         _num_workers = std::thread::hardware_concurrency();
      }
      assert(_num_workers > 0);
      _task_workers = new TaskWorker[_num_workers];
      // Tag each worker so it knows what thread it's running on
      for (uint32_t i = 0; i < _num_workers; ++i) {
         _task_workers[i].index = i;
      }

   }

   static uint32_t num_workers() {
      return _num_workers;
   }

   void reset() { _tasks_allocated = _bottom = _top = 0; }

   Task* allocate_task() {
      // return new Task();
      const uint32_t alloc_index = ++_tasks_allocated;
      return &_task_allocator[alloc_index & (TASKSCHED_ALLOC_SIZE - 1u)];
   }

   Task* get_task() {
      Task* task = pop();
      if (!task) {
         // our own queue is empty, steal from another, randomly
         int32_t random_index = udist(re);
         if (random_index == index) {
            // don't try to steal from ourselves, yield
            std::this_thread::yield();
            return nullptr;
         }
         TaskWorker& steal_worker = _task_workers[random_index];
         Task* stolen_task = steal_worker.steal();
         if (!stolen_task) {
            // we couldn't steal either, yield
            std::this_thread::yield();
            return nullptr;
         }

         // all good, return the stolen task
         return stolen_task;
      }

      return task;
   }

   void operator()() {
      active = true;
      while (active) {
         Task* task = get_task();
         if (task) {
            execute(task);
         }
      }
   }

   void execute(Task* task) {
      task->function(task, task->padding, index);
      finish(task);
   }

   void finish(Task* task) {
      const int32_t unfinished_tasks = --task->unfinished_tasks;
      if (unfinished_tasks == 0 && task->parent) {
         finish(task->parent);
      }
   }

   void push(Task* task) {
      std::lock_guard<std::mutex> lock(mtx_q);
      // queue.push_front(task);
      _task_queue[_bottom & QUEUE_MASK] = task;
      ++_bottom;
   }

   Task* pop() {
      std::lock_guard<std::mutex> lock(mtx_q);
      // if (queue.empty()) return nullptr;
      // Task* task = queue.front();
      // queue.pop_front();
      // return task;
      const int count = _bottom - _top;
      if (count <= 0) {
         return nullptr;
      }
      --_bottom;
      return _task_queue[_bottom & QUEUE_MASK];
   }

   Task* steal() {
      std::lock_guard<std::mutex> lock(mtx_q);
      // if (queue.empty()) return nullptr;
      // Task* task = queue.back();
      // queue.pop_back();
      // return task;
      const int count = _bottom - _top;
      if (count <= 0) {
         return nullptr;
      }
      Task* task = _task_queue[_top & QUEUE_MASK];
      ++_top;
      return task;
   }

   void shutdown() { active = false; }

};

TaskWorker* TaskWorker::_task_workers(0);
uint32_t TaskWorker::_num_workers(0);

void empty_job(Task*, void*, int thread_id) {
   // do nothing just count which thread we executed on
   g_thread_task_count[thread_id]++;
   // usleep(30);
}

int main(int argc, char** argv) {
   // std::cout << "sizeof(Task): " << sizeof(Task) << std::endl;
   // std::cout << "alignof(Task): " << alignof(Task) << std::endl;
   // std::cout << "sizeof(Task::padding): " << sizeof(Task::padding) <<
   // std::endl;

   TaskWorker::init();
   std::vector<std::thread> worker_threads;
   for (uint32_t i = 1; i < TaskWorker::num_workers(); ++i) {
      worker_threads.push_back(std::thread(std::ref(TaskWorker::_task_workers[i])));
   }

   static const int TEST_TASKS = 65000;
   Timer<std::chrono::milliseconds> timer;
   timer.start();
   // test serial job creation
   for (int i = 0; i < TEST_TASKS; ++i) {
      Task* task = TaskWorker::_task_workers[0].create_task(&empty_job);
      TaskWorker::_task_workers[0].run(task);
      TaskWorker::_task_workers[0].wait(task);
   }
   uint64_t tm_serial = timer.stop();
   std::cout << "Serial jobs took " << tm_serial << " ms" << std::endl;
   for (int i = 0; i < 8; ++i) {
      std::cout << "[" << i << "]: " << g_thread_task_count[i] << std::endl;
      g_thread_task_count[i] = 0;
      TaskWorker::_task_workers[i].reset();
   }

   // test parallel job creation
   Task* root = TaskWorker::_task_workers[0].create_task(&empty_job);
   for (int i = 0; i < TEST_TASKS - 1; ++i) {
      Task* task = TaskWorker::_task_workers[0].create_child_task(root, &empty_job);
      TaskWorker::_task_workers[0].run(task);
   }
   timer.start();
   TaskWorker::_task_workers[0].run(root);
   TaskWorker::_task_workers[0].wait(root);
   uint64_t tm_parallel = timer.stop();
   std::cout << "Parallel jobs took " << tm_parallel << " ms ("
             << float(tm_serial) / float(tm_parallel) << "x)" << std::endl;
   for (int i = 0; i < 8; ++i) {
      std::cout << "[" << i << "]: " << g_thread_task_count[i] << std::endl;
      g_thread_task_count[i] = 0;
   }

   for (uint32_t i = 1; i < TaskWorker::num_workers(); ++i) {
      TaskWorker::_task_workers[i].shutdown();
      worker_threads[i - 1].join();
   }

   return 0;
}
