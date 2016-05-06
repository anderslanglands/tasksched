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
using TaskQueue = std::deque<Task*>;

int g_num_workers = 0;
struct TaskWorker;
TaskWorker* g_task_workers = nullptr;
static const int TASKSCHED_MAX_JOBS = 65536;
static const int TASKSCHED_ALLOC_SIZE = TASKSCHED_MAX_JOBS;
//Task** g_task_allocator;
//__thread uint32_t g_tasks_allocated(0);

uint32_t g_thread_task_count[8] = {0};

struct TaskWorker {
   TaskWorker() : udist(0, g_num_workers - 1) {
      _tasks_allocated = 0;
      _task_allocator = new Task[TASKSCHED_ALLOC_SIZE];
   }

   ~TaskWorker() {
      delete[] _task_allocator;
   }

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

   static void init() {
      //g_task_allocator = new Task*[g_num_workers];
      //for (int i=0; i < g_num_workers; ++i) {
         //g_task_allocator[i] = new Task[TASKSCHED_ALLOC_SIZE];
      //}
   }

   void reset() {
      _tasks_allocated = 0;
   }

   Task* allocate_task() { 
      //return new Task(); 
      const uint32_t alloc_index = ++_tasks_allocated;
      return &_task_allocator[alloc_index & (TASKSCHED_ALLOC_SIZE-1u)]; 
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
         TaskWorker& steal_worker = g_task_workers[random_index];
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
      queue.push_front(task);
   }

   Task* pop() {
      std::lock_guard<std::mutex> lock(mtx_q);
      if (queue.empty()) return nullptr;
      Task* task = queue.front();
      queue.pop_front();
      return task;
   }

   Task* steal() {
      std::lock_guard<std::mutex> lock(mtx_q);
      if (queue.empty()) return nullptr;
      Task* task = queue.back();
      queue.pop_back();
      return task;
   }


   void shutdown() {
      active = false;
   }
   
   int32_t index;
   TaskQueue queue;
   std::atomic_bool active;
   std::random_device rd;
   std::default_random_engine re;
   std::uniform_int_distribution<int32_t> udist;
   std::mutex mtx_q;
   Task* _task_allocator;
   int _tasks_allocated;
};

void empty_job(Task*, void*, int thread_id) {
   // do nothing just count which thread we executed on
   g_thread_task_count[thread_id]++;
   //usleep(30);
}

int main(int argc, char** argv) {
   // std::cout << "sizeof(Task): " << sizeof(Task) << std::endl;
   // std::cout << "alignof(Task): " << alignof(Task) << std::endl;
   // std::cout << "sizeof(Task::padding): " << sizeof(Task::padding) <<
   // std::endl;

   g_num_workers = 8;
   TaskWorker::init();
   g_task_workers = new TaskWorker[g_num_workers];
   for (int i = 0; i < 8; ++i) {
      g_task_workers[i].index = i;
   }

   std::vector<std::thread> worker_threads;
   for (int i = 1; i < g_num_workers; ++i) {
      worker_threads.push_back(std::thread(std::ref(g_task_workers[i])));
   }

   static const int TEST_TASKS = 65000;
   Timer<std::chrono::milliseconds> timer;
   timer.start();
   // test serial job creation
   for (int i = 0; i < TEST_TASKS; ++i) {
      Task* task = g_task_workers[0].create_task(&empty_job);
      g_task_workers[0].run(task);
      g_task_workers[0].wait(task);
   }
   uint64_t tm_serial = timer.stop();
   std::cout << "Serial jobs took " << tm_serial << " ms" << std::endl;
   for (int i = 0; i < 8; ++i) {
      std::cout << "[" << i << "]: " << g_thread_task_count[i] << std::endl;
      g_thread_task_count[i] = 0;
      g_task_workers[i].reset();
   }

   // test parallel job creation
   Task* root = g_task_workers[0].create_task(&empty_job);
   for (int i = 0; i < TEST_TASKS - 1; ++i) {
      Task* task = g_task_workers[0].create_child_task(root, &empty_job);
      g_task_workers[0].run(task);
   }
   timer.start();
   g_task_workers[0].run(root);
   g_task_workers[0].wait(root);
   uint64_t tm_parallel = timer.stop();
   std::cout << "Parallel jobs took " << tm_parallel << " ms (" << float(tm_serial) / float(tm_parallel) << "x)" << std::endl;
   for (int i = 0; i < 8; ++i) {
      std::cout << "[" << i << "]: " << g_thread_task_count[i] << std::endl;
      g_thread_task_count[i] = 0;
   }


   for (int i = 1; i < g_num_workers; ++i) {
      g_task_workers[i].shutdown();
      worker_threads[i-1].join();
   }
     

   return 0;
}
