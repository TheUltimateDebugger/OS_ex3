#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"
#include <atomic>
#include <pthread.h>
#include <semaphore.h>


struct ThreadContext {
    JobState* job_state;
    const MapReduceClient& client;
    std::atomic<int>* atomic_index;
    std::atomic<int>* atomic_progress;
    pthread_t thread_id;
    const InputVec& input_vec;
    IntermediateVec intermediate_vector;
    OutputVec& output_vec;
    Barrier* barrier;
    ThreadContext(const MapReduceClient& client, std::atomic<int>* index, std::atomic<int>* progress, const InputVec& inputVec, OutputVec& outputVec, JobState* job_state1, Barrier* barrier_1)
            : job_state(job_state1), client(client), atomic_index(index), atomic_progress(progress), input_vec(inputVec), output_vec(outputVec), barrier(barrier_1) {}
};

struct JobContext {
    JobState* job_state;
    int length;
    const InputVec& input_vec;
    OutputVec& output_vec;
    std::vector<ThreadContext*> thread_contexts;
    pthread_mutex_t job_mutex;
    pthread_cond_t job_cv;
    bool job_done;

    JobContext(const InputVec& inputVec, OutputVec& outputVec, JobState* job_state1)
            :job_state(job_state1), input_vec(inputVec), output_vec(outputVec), job_done(false) {
        length = inputVec.size();
        pthread_mutex_init(&job_mutex, nullptr);
        pthread_cond_init(&job_cv, nullptr);
    }

    ~JobContext() {
        for (auto& tc : thread_contexts) {
            delete tc;
        }
        pthread_mutex_destroy(&job_mutex);
        pthread_cond_destroy(&job_cv);
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
    ThreadContext* tc = (ThreadContext*) context;
    tc->job_state->stage = SHUFFLE_STAGE;
    tc->job_state->percentage = 0;
    //make everyone start at the same time
    tc->barrier->barrier();
    while(true)
    {
        int old_value = (*(tc->atomic_index))++;
        if (old_value >= tc->input_vec.size())
            break;

        MapReduceClient::map(tc->input_vec[old_value].first,
                             tc->input_vec[old_value].second, context);
    }
    //waits for everyone to finish reading before updating the job state
    tc->barrier->barrier();
    tc->atomic_index = 0;
    tc->atomic_progress = 0;
    tc->job_state->stage = SHUFFLE_STAGE;
    tc->job_state->percentage = 0;
    //start sorting
    tc->barrier->barrier();
    //TODO: implement sorting
    //finish sorting
    tc->barrier->barrier();
    //TODO: implement shuffle
    tc->job_state->stage = REDUCE_STAGE;
    tc->job_state->percentage = 0;
    //release minions from shuffle stage
    tc->barrier->barrier();
    while(true)
    {
      int old_value = (*(tc->atomic_index))++;
      if (old_value >= tc->input_vec.size())
        break;

      std::sort(intermidiate_vector)
      //we continue to shuffle only after everyone sorted their data
      tc->barrier->barrier();


      MapReduceClient::(tc->input_vec[old_value].first,
                           tc->input_vec[old_value].second, context);
    }

    return NULL;
}

void* Minion_thread(void* arg)
{
    ThreadContext* tc = (ThreadContext*) context;
    //waits for everyone to start at the same time
    tc->barrier->barrier();
    while(true)
    {
        int old_value = (*(tc->atomic_index))++;
        if (old_value >= tc->input_vec.size())
            break;

        MapReduceClient::map(tc->input_vec[old_value].first,
                             tc->input_vec[old_value].second, context);
    }
    //waits for everyone to finish mapping
    tc->barrier->barrier();
    //wait for boss to signal to start sorting
    tc->barrier->barrier();
    //TODO: implement sorting
    //wait for everyone to finish sorting
    tc->barrier->barrier();
    //wait for boss to finish shuffling
    tc->barrier->barrier();

    while(true)
    {
        int old_value = (*(tc->atomic_index))++;
        if (old_value >= tc->input_vec.size())
            break;

        std::sort(intermidiate_vector)
        //we continue to shuffle only after everyone sorted their data
        tc->barrier->barrier();


        MapReduceClient::(tc->input_vec[old_value].first,
                          tc->input_vec[old_value].second, context);
    }

    return NULL;
}

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel){
    // Create a ThreadContext....
    std::atomic<int>* atomic_index(0);
    std::atomic<int>* atomic_progress(0);
    JobState* job_state = new JobState;
    job_state->percentage = 0;
    job_state->stage = UNDEFINED_STAGE;
    Barrier* barrier = new Barrier(multiThreadLevel);
    JobContext* job_context = new JobContext(inputVec, outputVec, job_state);

    // Create and start threads
    for (int i = 0; i < multiThreadLevel; ++i) {
        ThreadContext* thread_context = new ThreadContext(client, atomic_index, atomic_progress, inputVec, outputVec, job_state, barrier);
        job_context->thread_contexts.push_back(thread_context);
        if (i == 0) {
            pthread_create(&thread_context->thread_id, nullptr, Boss_thread, thread_context);
        } else {
            pthread_create(&thread_context->thread_id, nullptr, Minion_thread, thread_context);
        }
    }

    return static_cast<JobHandle>(job_context);
}

void waitForJob(JobHandle job){
    JobContext* job_context = (JobContext*)job;
    pthread_mutex_lock(&job_context->job_mutex);

    // Wait until the job is done
    while (!job_context->job_done) {
        pthread_cond_wait(&job_context->job_cv, &job_context->job_mutex);
    }

    // Join all threads to ensure they are done
    for (auto& tc : job_context->thread_contexts) {
        pthread_join(tc->thread_id, nullptr);
    }

    pthread_mutex_unlock(&job_context->job_mutex);
}

void getJobState(JobHandle job, JobState* state);

void closeJobHandle(JobHandle job);
