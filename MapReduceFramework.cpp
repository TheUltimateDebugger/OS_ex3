#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"
#include <atomic>
#include <pthread.h>
#include <semaphore.h>


struct ThreadContext {
    const MapReduceClient& client;
    std::atomic<int>* atomic_index;
    std::atomic<int>* atomic_progress;
    pthread_t thread_id;
    const InputVec& input_vec;
    IntermediateVec intermediate_vector;
    OutputVec& output_vec;

    ThreadContext(const MapReduceClient& client, std::atomic<int>* index, std::atomic<int>* progress, const InputVec& inputVec, OutputVec& outputVec)
            : client(client), atomic_index(index), atomic_progress(progress), input_vec(inputVec), output_vec(outputVec) {}
};

struct JobContext {
    JobState job_state;
    int length;
    const InputVec& input_vec;
    OutputVec& output_vec;
    std::vector<ThreadContext*> thread_contexts;

    JobContext(const InputVec& inputVec, OutputVec& outputVec)
            : input_vec(inputVec), output_vec(outputVec) {
        job_state = {UNDEFINED_STAGE, 0};
        length = inputVec.size();
    }

    ~JobContext() {
        for (auto& tc : thread_contexts) {
            delete tc;
        }
    }
};

void emit2(K2* key, V2* value, void* context) {
    ThreadContext* tc = (ThreadContext*)context;
    tc->intermediate_vector.push_back(IntermediatePair(key, value));
    (*(tc->atomic_progress))++;
}

void emit3(K3* key, V3* value, void* context) {
    ThreadContext* tc = (ThreadContext*)context;
    tc->output_vec.emplace_back(key, value);
    (*(tc->atomic_progress))++;
}

void* Boss_thread(void* arg){
    return NULL;
}

void* Minion_thread(void* arg)
{

    return NULL;
}

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel){
    // Create a ThreadContext....
    std::atomic<int>* atomic_index(0);
    std::atomic<int>* atomic_progress(0);
    JobContext* job_context = new JobContext(inputVec, outputVec);

    // Create and start threads
    for (int i = 0; i < multiThreadLevel; ++i) {
        ThreadContext* thread_context = new ThreadContext(client, atomic_index, atomic_progress, inputVec, outputVec);
        job_context->thread_contexts.push_back(thread_context);
        if (i == 0) {
            pthread_create(&thread_context->thread_id, nullptr, Boss_thread, thread_context);
        } else {
            pthread_create(&thread_context->thread_id, nullptr, Minion_thread, thread_context);
        }
    }

    return static_cast<JobHandle>(job_context);
}

void waitForJob(JobHandle job);

void getJobState(JobHandle job, JobState* state);

void closeJobHandle(JobHandle job);
