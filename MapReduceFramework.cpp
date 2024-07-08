#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"
#include <atomic>
#include <pthread.h>
#include <semaphore.h>


typedef struct ThreadContext{
    MapReduceClient& client;
    std::atomic<int>* atomic_index;
    std::atomic<int>* atomic_progress;
    pthread_t thread_id;
    InputPair input_pair;
    IntermediatePair intermediate_pair;
    OutputPair output_pair;
}ThreadContext;

typedef struct JobContext{
    JobState job_state;
    int length;
    InputVec& input_vec;
    IntermediateVec intermediate_vec;
    OutputVec& output_vec;
}JobContext;


void emit2 (K2* key, V2* value, void* context){
    ThreadContext *tc = (ThreadContext*) context;
    tc->intermediate_vec.push_back(IntermediatePair(key, value));
    int old_value = (*(tc->atomic_progress))++;
}

void emit3 (K3* key, V3* value, void* context){
    ThreadContext* tc = (ThreadContext*) context;
    tc->output_vec.emplace_back(key, value);
    int old_value = (*(tc->atomic_progress))++;
}

void* Boss_thread(void* arg){
    return NULL;
}

void* Minion_thread(void* arg){
    return NULL;
}

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel){
//    Create a ThreadContext....
    std::atomic<int>* atomic_index(0);
    std::atomic<int>* atomic_progress(0);
    ThreadContext* context = new ThreadContext(client, atomic_counter, 0, inputVec, nullptr, outputVec);


    for (int i = 0; i < multiThreadLevel - 1; ++i)
    {
        pthread_create(&(context->thread_id), nullptr, MapReduce, context);
    }
}

void waitForJob(JobHandle job);

void getJobState(JobHandle job, JobState* state);

void closeJobHandle(JobHandle job);
