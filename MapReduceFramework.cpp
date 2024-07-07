#include "MapReduceFramework.h"

type std::vector<std::pair<K1*,V1*>>;
type std::vector<std::pair<K2*,V2*>>;
type std::vector<std::pair<K3,V3*>>;

void emit2 (K2* key, V2* value, void* context);
void emit3 (K3* key, V3* value, void* context);

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel);

void waitForJob(JobHandle job);

void getJobState(JobHandle job, JobState* state);

void closeJobHandle(JobHandle job);