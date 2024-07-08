#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <atomic>
#include <pthread.h>
#include <semaphore.h>


struct ThreadContext{
    MapReduceClient& client;
    std::atomic<int>* atomic_counter;
    pthread_t thread_id;
    InputVec& input_vec;
    IntermediateVec intermediate_vec;
    OutputVec& output_vec;
    JobState job_state;

};


void emit2 (K2* key, V2* value, void* context){
    ThreadContext *tc = (ThreadContext*) context;
    tc->intermediate_vec.push_back(IntermediatePair(key, value));
    tc->atomic_counter++;
}

void emit3 (K3* key, V3* value, void* context){
    ThreadContext* tc = (ThreadContext*) context;
    tc->output_vec.emplace_back(key, value);
    tc->atomic_counter++;
}

void* MapReduce(void* arg){
    return NULL;
}

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel){
//    Create a ThreadContext....
    std::atomic<int>* atomic_counter(0);
    ThreadContext* context = new ThreadContext(client, atomic_counter, 0, inputVec, nullptr, outputVec);


    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_create(&(context->thread_id), nullptr, MapReduce, context);
    }
}

void waitForJob(JobHandle job);

void getJobState(JobHandle job, JobState* state);

void closeJobHandle(JobHandle job);