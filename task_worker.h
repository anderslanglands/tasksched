#pragma once

#include <functional>
#include <atomic>
#include <iostream>
#include <random>
#include <thread>
#include <cassert>
#include <unistd.h>

struct Task {
   using Function = std::function<void(Task*, void*, int)>;
   Function function;
   Task* parent;
   std::atomic_int unfinished_tasks;
   uint8_t padding[128 - sizeof(Function) - sizeof(Task*) -
                   sizeof(std::atomic_int)];

   bool has_completed() const { return unfinished_tasks <= 0; }
};

#define COMPILER_BARRIER asm volatile("" ::: "memory")
#define MEMORY_BARRIER asm volatile("mfence" ::: "memory")

struct TaskWorker {
   int32_t index;
   // TaskQueue queue;
   static const uint32_t QUEUE_SIZE = 65536;
   static const uint32_t QUEUE_MASK = QUEUE_SIZE - 1u;
   static const int TASKSCHED_MAX_JOBS = 65536;
   static const int TASKSCHED_ALLOC_SIZE = TASKSCHED_MAX_JOBS;
   Task* _task_queue[QUEUE_SIZE];
   std::atomic<uint32_t> _bottom;
   std::atomic<uint32_t> _top;
   std::atomic_bool active;
   std::random_device rd;
   std::default_random_engine re;
   std::uniform_int_distribution<int32_t> udist;
   std::mutex mtx_q;
   Task* _task_allocator;
   int _tasks_allocated;

   static uint32_t _num_workers;
   static TaskWorker* _task_workers;

   static std::vector<std::thread> _worker_threads;

   TaskWorker() : _bottom(0), _top(0), udist(0, _num_workers - 1) {
      _tasks_allocated = 0;
      _task_allocator = new Task[TASKSCHED_ALLOC_SIZE];
   }

   ~TaskWorker() { delete[] _task_allocator; }

   Task* _create_task(Task::Function function) {
      Task* task = allocate_task();
      task->function = function;
      task->parent = nullptr;
      task->unfinished_tasks = 1;
      return task;
   }

   static Task* create_task(Task::Function function) {
      return _task_workers[0]._create_task(function);
   }

   Task* _create_child_task(Task* parent, Task::Function function) {
      parent->unfinished_tasks++;
      Task* task = allocate_task();
      task->function = function;
      task->parent = parent;
      task->unfinished_tasks = 1;
      return task;
   }

   static Task* create_child_task(Task* parent, Task::Function function) {
      return _task_workers[0]._create_child_task(parent, function);
   }

   void _run(Task* task) { push(task); }

   static void run(Task* task) {
      _task_workers[0]._run(task);
   }

   void _wait(const Task* task) {
      while (!task->has_completed()) {
         Task* next_task = get_task();
         if (next_task) {
            execute(next_task);
         }
      }
   }

   static void wait(const Task* task) {
      _task_workers[0]._wait(task);
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

      // Start the threads
      _worker_threads.resize(num_threads-1);
      for (uint32_t i = 1; i < _num_workers; ++i) {
         _worker_threads[i-1] = std::thread(std::ref(_task_workers[i]));
      }
   }

   static void shutdown() {
      for (uint32_t i = 1; i < _num_workers; ++i) {
         _task_workers[i]._shutdown();
         _worker_threads[i - 1].join();
      }

      delete[] _task_workers;
      _task_workers = nullptr;
      _worker_threads.resize(0);

   }

   static uint32_t num_workers() { return _num_workers; }

   void reset() { _tasks_allocated = _bottom = _top = 0; }

   Task* allocate_task() {
      // return new Task();
      const uint32_t alloc_index =
          (++_tasks_allocated) & (TASKSCHED_ALLOC_SIZE - 1u);
      uint32_t* dp = (uint32_t*)_task_allocator[alloc_index].padding;
      *dp = alloc_index;
      return &_task_allocator[alloc_index];
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
      uint32_t b = _bottom;
      _task_queue[b & QUEUE_MASK] = task;
      COMPILER_BARRIER;
      _bottom = b + 1;
   }

   Task* pop() {
      uint32_t b = _bottom - 1;
      _bottom = b;
      MEMORY_BARRIER;
      uint32_t t = _top;
      if (t <= b) {
         Task* task = _task_queue[b & QUEUE_MASK];
         if (t != b) {
            // there's still more than one item left in the queue
            return task;
         }

         // this is the last item in the queue
         if (!_top.compare_exchange_strong(t, t + 1)) {
            // lost the race against steal() in another thread
            task = nullptr;
         }

         // set the queue empty
         _bottom = t + 1;
         return task;
      } else {
         // queue was already empty
         _bottom = t;
         return nullptr;
      }
   }

   Task* steal() {
      uint32_t t = _top;
      COMPILER_BARRIER;
      uint32_t b = _bottom;
      if (t < b) {
         // non-empty queue
         Task* task = _task_queue[t & QUEUE_MASK];
         if (!_top.compare_exchange_strong(t, t + 1)) {
            // another thread removed the element in the meantime
            return nullptr;
         }
         return task;
      } else {
         return nullptr;
      }
   }

   void _shutdown() { active = false; }
};

#undef COMPILER_BARRIER
#undef MEMORY_BARRIER
