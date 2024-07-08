#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"
#include <atomic>
#include "pthread.h"
#include <semaphore.h>
#include <algorithm>


struct ThreadContext {
    JobState* job_state;
    int thread_index;
    pthread_mutex_t *grade_lock;
    const MapReduceClient& client;
    std::atomic<int>* atomic_index;
    pthread_t thread_id;
    const InputVec& input_vec;
    std::vector<IntermediateVec> intermediate_super_vector;
    OutputVec& output_vec;
    Barrier* barrier;
    ThreadContext(const MapReduceClient& client, std::atomic<int>* index,
                  const InputVec& inputVec, OutputVec& outputVec, JobState*
    job_state1, Barrier*
    barrier_1, int i, pthread_mutex_t *lock)
            : job_state(job_state1), client(client), atomic_index(index),
              input_vec(inputVec), output_vec
                      (outputVec), barrier(barrier_1), thread_index(i), grade_lock(lock) {}
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
    Barrier *barrier;

    JobContext(const InputVec &inputVec, OutputVec &outputVec, JobState *job_state1, int multiThreadLevel)
            : job_state(job_state1), input_vec(inputVec), output_vec(outputVec), job_done(false)
    {
        length = inputVec.size();
        pthread_mutex_init(&job_mutex, nullptr);
        pthread_cond_init(&job_cv, nullptr);
        barrier = new Barrier(multiThreadLevel); // Initialize the barrier
    }

    ~JobContext()
    {
        for (auto &tc: thread_contexts)
        {
            delete tc;
        }
        pthread_mutex_destroy(&job_mutex);
        pthread_cond_destroy(&job_cv);
        delete barrier; // Clean up the barrier
    }
};

void emit2(K2* key, V2* value, void* context) {
    ThreadContext* tc = (ThreadContext*)context;
    tc->intermediate_super_vector[tc->thread_index].push_back(IntermediatePair(key, value));
}

void emit3(K3* key, V3* value, void* context) {
    ThreadContext* tc = (ThreadContext*)context;
    tc->output_vec.emplace_back(key, value);
}

void* Boss_thread(void* arg){
    ThreadContext* tc = (ThreadContext*) arg;
    tc->job_state->stage = SHUFFLE_STAGE;
    tc->job_state->percentage = 0;
    //make everyone start at the same time
    tc->barrier->barrier();
    while(true)
    {
        int old_value = (*(tc->atomic_index))++;
        if (old_value >= tc->input_vec.size())
            break;

        tc->client.map(tc->input_vec[old_value].first,
                             tc->input_vec[old_value].second, arg);
        //updates percentage, no one can touch it in this time
        pthread_mutex_lock(tc->grade_lock);
        int value = tc->atomic_index->load();
        tc->job_state->percentage = ((float)value / (float)tc->input_vec.size()) * 100;
        pthread_mutex_unlock(tc->grade_lock);
    }
    //waits for everyone to finish reading before updating the job state
    tc->barrier->barrier();
    tc->atomic_index = 0;
    tc->job_state->stage = SHUFFLE_STAGE;
    tc->job_state->percentage = 0;
    //start sorting
    tc->barrier->barrier();
    std::sort(tc->intermediate_super_vector[tc->thread_index].begin(), tc->intermediate_super_vector[tc->thread_index].end());
    //finish sorting
    tc->barrier->barrier();
    //TODO: implement shuffle
    tc->job_state->stage = REDUCE_STAGE;
    tc->job_state->percentage = 0;
    //release minions from shuffle stage
    tc->barrier->barrier();
    //TODO: implement reduce

    return NULL;
}

void* Minion_thread(void* arg)
{
    ThreadContext* tc = (ThreadContext*) arg;
    //waits for everyone to start at the same time
    tc->barrier->barrier();
    while(true)
    {
        int old_value = (*(tc->atomic_index))++;
        if (old_value >= tc->input_vec.size())
            break;

        tc->client.map(tc->input_vec[old_value].first,
                             tc->input_vec[old_value].second, arg);
      //updates percentage, no one can touch it in this time
        pthread_mutex_lock(tc->grade_lock);
        int value = tc->atomic_index->load();
        tc->job_state->percentage = ((float)value / (float)tc->input_vec.size()) * 100;
        pthread_mutex_unlock(tc->grade_lock);
    }
    //waits for everyone to finish mapping
    tc->barrier->barrier();
    //wait for boss to signal to start sorting
    tc->barrier->barrier();
    std::sort(tc->intermediate_super_vector[tc->thread_index].begin(), tc->intermediate_super_vector[tc->thread_index].end());
    //wait for everyone to finish sorting
    tc->barrier->barrier();
    //wait for boss to finish shuffling
    tc->barrier->barrier();
    //TODO: implement reduce


    return NULL;
}

JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel)
{
    // Create atomic variables
    std::atomic<int> *atomic_index = new std::atomic<int>(0);

    // Initialize job state
    JobState *job_state = new JobState;
    job_state->percentage = 0;
    job_state->stage = UNDEFINED_STAGE;

    // Initialize barrier
    Barrier* barrier = new Barrier(multiThreadLevel);

    // Create JobContext
    JobContext *job_context = new JobContext(inputVec, outputVec, job_state, multiThreadLevel);

    // Initialize mutex
    pthread_mutex_t* grade_lock = new pthread_mutex_t;
    pthread_mutex_init(grade_lock, NULL);

    // Create and start threads
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        ThreadContext *thread_context = new ThreadContext(client, atomic_index, inputVec, outputVec,
                                                          job_state, barrier, i, grade_lock);
        job_context->thread_contexts.push_back(thread_context);
        if (i == 0)
        {
            pthread_create(&thread_context->thread_id, nullptr, Boss_thread, thread_context);
        } else
        {
            pthread_create(&thread_context->thread_id, nullptr, Minion_thread, thread_context);
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

    // Join all threads to ensure they are done
    for (auto &tc: job_context->thread_contexts)
    {
        pthread_join(tc->thread_id, nullptr);
    }

    // Clean up resources
    delete job_context->job_state;
    delete job_context->barrier; // Clean up the barrier

    // The destructor of JobContext will clean up thread contexts and destroy the mutex and condition variable
    delete job_context;
}

