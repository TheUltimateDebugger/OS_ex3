#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"
#include <atomic>
#include "pthread.h"
#include <semaphore.h>
#include <algorithm>
#include <cstdio>
#include <iostream>

struct JobContext;

struct ThreadContext
{
    JobState *job_state;
    int thread_index;
    pthread_mutex_t *grade_lock;
    pthread_mutex_t *emit3_lock;
    const MapReduceClient &client;
    std::atomic<int> &atomic_index;
    std::atomic<int> &atomic_index2;
    pthread_t thread_id;
    const InputVec &input_vec;
    std::vector<IntermediateVec> &intermediate_super_vector;
    std::vector<IntermediateVec> &intermediate_shuffled_super_vector;
    OutputVec &output_vec;
    Barrier *barrier;
    JobContext *job_context;

    ThreadContext(const MapReduceClient &client, std::atomic<int> &index, std::atomic<int> &index2, const InputVec &inputVec,
                  OutputVec &outputVec,
                  JobState *job_state1, Barrier *barrier_1, int i,
                  pthread_mutex_t *lock, pthread_mutex_t *emit3,
                  std::vector<IntermediateVec> &inter_vector, std::vector<IntermediateVec> &shuffled_vector, JobContext *jc)
        : job_state(job_state1), client(client), atomic_index(index), atomic_index2(index2), input_vec(inputVec), output_vec(outputVec),
          barrier(barrier_1), thread_index(i), grade_lock(lock), emit3_lock(emit3),
          intermediate_super_vector(inter_vector),
          intermediate_shuffled_super_vector(shuffled_vector), job_context(jc)
    {}
};

struct JobContext
{
    JobState *job_state;
    int length;
    const InputVec &input_vec;
    OutputVec &output_vec;
    std::vector<ThreadContext *> thread_contexts;
    pthread_mutex_t job_mutex;
    pthread_cond_t job_cv;
    bool job_done;
    std::atomic<int> atomic_index;
    std::atomic<int> atomic_index2;
    int size;

    JobContext(const InputVec &inputVec, OutputVec &outputVec, JobState *job_state1, int multiThreadLevel)
        : job_state(job_state1), input_vec(inputVec), output_vec(outputVec), job_done(false), atomic_index(0), atomic_index2(0)
    {
      length = inputVec.size();
      pthread_mutex_init(&job_mutex, nullptr);
      pthread_cond_init(&job_cv, nullptr);
    }
};

void MemoryFree(JobContext *jc);

void emit2(K2 *key, V2 *value, void *context)
{
  ThreadContext *tc = (ThreadContext *) context;
  tc->intermediate_super_vector[tc->thread_index].push_back(IntermediatePair(key, value));
}

void emit3(K3 *key, V3 *value, void *context)
{
  ThreadContext *tc = (ThreadContext *) context;
  pthread_mutex_lock(tc->emit3_lock);
  tc->output_vec.emplace_back(key, value);
  pthread_mutex_unlock(tc->emit3_lock);
}

void *Boss_thread(void *arg)
{
  ThreadContext *tc = (ThreadContext *) arg;
  tc->job_state->stage = SHUFFLE_STAGE;
  tc->job_state->percentage = 0;
  //make everyone start at the same time
  tc->barrier->barrier();
  while (true)
  {
    int old_value = tc->atomic_index++;
//        int old_value = 0;
    if (old_value >= tc->input_vec.size())
      break;
    tc->client.map(tc->input_vec[old_value].first, tc->input_vec[old_value].second, arg);
    //updates percentage, no one can touch it in this time
    pthread_mutex_lock(tc->grade_lock);
    int new_val = ++tc->atomic_index2;
    tc->job_state->percentage = ((float) new_val / (float) tc->input_vec.size()) * 100;
    pthread_mutex_unlock(tc->grade_lock);
  }
  //waits for everyone to finish reading before updating the job state
  tc->barrier->barrier();
  tc->atomic_index = 0;
  tc->atomic_index2 = 0;
  //start sorting
  tc->barrier->barrier();
  std::sort(tc->intermediate_super_vector[tc->thread_index].begin(),
            tc->intermediate_super_vector[tc->thread_index].end());
  //finish sorting
  tc->barrier->barrier();
  tc->job_state->stage = SHUFFLE_STAGE;
  tc->job_state->percentage = 0;
  int counter = 0;
  int size = 0;
  for (int i = 0; i < tc->intermediate_super_vector.size(); i++){
    size += tc->intermediate_super_vector[i].size();
  }
  tc->job_context->size = size;
  while (counter < size)
  {
    K2 *max_key = nullptr;
    for (const auto &vec: tc->intermediate_super_vector)
    {
      if (!vec.empty())
      {
        const IntermediatePair &last_pair = vec.back();
        K2 *current_key = last_pair.first;

        if (max_key == nullptr || (*current_key < *max_key))
        {
          max_key = current_key;
        }
      }
    }
    if (max_key == nullptr)
      break;
    bool opend_for_max_key = false;
    for (auto &vec: tc->intermediate_super_vector)
    {
      if (!vec.empty())
      {
        IntermediatePair &last_pair = vec.back();
        if (!(*last_pair.first < *max_key || *max_key < *last_pair.first))
        {
          if (!opend_for_max_key)
          {
            tc->intermediate_shuffled_super_vector.push_back
                ({last_pair});
            opend_for_max_key = true;
            vec.pop_back();
          } else
          {
            tc->intermediate_shuffled_super_vector[tc->intermediate_shuffled_super_vector.size() - 1]
                .push_back(last_pair);
            vec.pop_back();
          }
          counter++;
          tc->job_state->percentage = (float)counter / (float)size
                                      * 100;
        }
      }
    }

  }
  tc->job_state->stage = REDUCE_STAGE;
  tc->job_state->percentage = 0;
  //release minions from shuffle stage
  tc->barrier->barrier();
  while (true)
  {
    int old_value = tc->atomic_index++;
    if (old_value >= size)
      break;

    tc->client.reduce(&tc->intermediate_shuffled_super_vector[old_value], arg);
    //updates percentage, no one can touch it in this time
    pthread_mutex_lock(tc->grade_lock);
    tc->job_state->percentage = ((float) (old_value + 1)  / (float) size) *100;
    pthread_mutex_unlock(tc->grade_lock);
  }

  tc->barrier->barrier();
  tc->job_context->job_done = true;
  return nullptr;
}

void *Minion_thread(void *arg)
{
  ThreadContext *tc = (ThreadContext *) arg;
  //waits for everyone to start at the same time
  tc->barrier->barrier();
  while (true)
  {
    int old_value = tc->atomic_index++;
    if (old_value >= tc->input_vec.size())
      break;

    std::cout << tc->input_vec[old_value].first << std::endl;
    tc->client.map(tc->input_vec[old_value].first,
                   tc->input_vec[old_value].second, arg);
    //updates percentage, no one can touch it in this time
    pthread_mutex_lock(tc->grade_lock);
    int new_val = ++tc->atomic_index2;
    tc->job_state->percentage = ((float) new_val / (float) tc->input_vec.size()) * 100;
    pthread_mutex_unlock(tc->grade_lock);
  }
  //waits for everyone to finish mapping
  tc->barrier->barrier();
  //wait for boss to signal to start sorting
  tc->barrier->barrier();
  std::sort(tc->intermediate_super_vector[tc->thread_index].begin(),
            tc->intermediate_super_vector[tc->thread_index].end(),
            [](const IntermediatePair& a, const IntermediatePair& b) {
                return *(a.first) < *(b.first);
            });
  //wait for everyone to finish sorting
  tc->barrier->barrier();
  //wait for boss to finish shuffling
  tc->barrier->barrier();
  pthread_mutex_lock(tc->grade_lock);
  int size = tc->job_context->size;
  pthread_mutex_unlock(tc->grade_lock);
  while (true)
  {
    int old_value = tc->atomic_index++;
    if (old_value >= size)
      break;

    tc->client.reduce(&tc->intermediate_shuffled_super_vector[old_value], arg);
    //updates percentage, no one can touch it in this time
    pthread_mutex_lock(tc->grade_lock);
    tc->job_state->percentage = ((float) (old_value + 1) / (float) size) * 100;
    pthread_mutex_unlock(tc->grade_lock);
  }
  tc->barrier->barrier();
  tc->barrier->barrier();
  return nullptr;
}

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
{
  // Initialize job state
  JobState *job_state = new JobState;
  job_state->percentage = 0;
  job_state->stage = UNDEFINED_STAGE;

  // Initialize barrier
  Barrier *barrier = new Barrier(multiThreadLevel);

  // Create JobContext
  JobContext *job_context = new JobContext(inputVec, outputVec, job_state, multiThreadLevel);

  // Initialize mutex
  pthread_mutex_t *grade_lock = new pthread_mutex_t;
  pthread_mutex_t *emit3_lock = new pthread_mutex_t;
  pthread_mutex_init(grade_lock, NULL);
  pthread_mutex_init(emit3_lock, NULL);


  std::vector<IntermediateVec> *inter_vec = new std::vector<IntermediateVec>;
  std::vector<IntermediateVec> *shuffle_vec = new std::vector<IntermediateVec>;

  for (int i = 0; i < multiThreadLevel; ++i){
    IntermediateVec inter = IntermediateVec();
    inter_vec->push_back(inter);
  }

  // Create and start threads
  for (int i = 0; i < multiThreadLevel; ++i)
  {
    ThreadContext *thread_context = new ThreadContext(client, job_context->atomic_index, job_context->atomic_index2, inputVec, outputVec,
                                                      job_state, barrier,
                                                      i, grade_lock,
                                                      emit3_lock,
                                                      *inter_vec,
                                                      *shuffle_vec, job_context);
    job_context->thread_contexts.push_back(thread_context);
    if (i == 0)
    {
      if (pthread_create(&thread_context->thread_id, nullptr, Boss_thread,
                         thread_context) != 0)
      {
        fprintf(stderr, "system error: thread creation failed\n");
        MemoryFree(job_context);
        exit(1);
      }
    } else
    {
      if (pthread_create(&thread_context->thread_id, nullptr, Minion_thread, thread_context) != 0)
      {
        fprintf(stderr, "system error: thread creation failed\n");
        MemoryFree(job_context);
        exit(1);
      }

    }
  }

  return static_cast<JobHandle>(job_context);
}


void waitForJob(JobHandle job)
{
  JobContext *job_context = (JobContext *) job;
  pthread_mutex_lock(&job_context->job_mutex);

  // Wait until the job is done
  while (!job_context->job_done)
  {
    pthread_cond_wait(&job_context->job_cv, &job_context->job_mutex);
  }

  // Join all threads to ensure they are done
  for (auto &tc: job_context->thread_contexts)
  {
    pthread_join(tc->thread_id, nullptr);
  }

  pthread_mutex_unlock(&job_context->job_mutex);
}

void getJobState(JobHandle job, JobState *state)
{
  JobContext *jc = (JobContext *) job;
  *state = *(jc->job_state);
}

void MemoryFree(JobContext *jc)
{
  // Free memory allocated for thread contexts
  delete &jc->thread_contexts[0]->intermediate_super_vector;
  delete &jc->thread_contexts[0]->intermediate_shuffled_super_vector;
//    delete jc->thread_contexts[0]->job_state;
//    delete jc->thread_contexts[0]->barrier; // Free the barrier

  for (auto &tc : jc->thread_contexts)
  {
    // Free thread context itself
    delete tc;
  }

  // Free the locks
//    pthread_mutex_destroy(jc->thread_contexts[0]->grade_lock);
//    pthread_mutex_destroy(jc->thread_contexts[0]->emit3_lock);
//    delete jc->thread_contexts[0]->grade_lock;
//    delete jc->thread_contexts[0]->emit3_lock;
}

void closeJobHandle(JobHandle job)
{
  JobContext *job_context = (JobContext *) job;

  // Lock the mutex to safely check and wait for job completion
  pthread_mutex_lock(&job_context->job_mutex);

  // If the job is not done, wait for it to finish
  while (!job_context->job_done)
  {
    pthread_cond_wait(&job_context->job_cv, &job_context->job_mutex);
  }

  pthread_mutex_unlock(&job_context->job_mutex);

  // Call MemoryFree to clean up thread contexts and their resources
  MemoryFree(job_context);

  // The destructor of JobContext will clean up thread contexts and destroy the mutex and condition variable
  delete job_context;
}

