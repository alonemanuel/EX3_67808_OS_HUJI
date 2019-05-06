#include <pthread.h>
#include "MapReduceFramework.h"

typdef struct JobContext
{
	pthread_t *threads;
	JobState state;

	JobContext(pthread_t *_threads) : threads(_threads)
	{
		// Init state
	}
	// More things
} JobContext;


void emit2(K2 *key, V2 *value, void *context)
{

}

void emit3(K3 *key, V3 *value, void *context)
{

}

void threadMapReduce((std::vector<std::pair<K1*, V1*>>*) inputVecP,
					 (int*) interVec_size,
					 (std::vector<std::pair<K3*, V3*>>*) outputVecP)
{
	std::vector<std::pair<K1*, V1*>> inputVec = *inputVecP;
	std::vector<std::pair<K2*, V2*>> inter =
			new std::vector<std::pair<K2*, V2*>>(*interVec_size);
	std::vector<std::pair<K3*, V3*>> outputVec = *outputVecP;

	for (){

	}
	// map
	// reduce
}


JobHandle startMapReduceJob(const MapReduceClient &client,
							const InputVec &inputVec, OutputVec &outputVec,
							int multiThreadLevel)
{
	pthread_t threads[multiThreadLevel];
	ThreadContext contexts[multiThreadLevel];

	for (int i = 0; i < multiThreadLevel; ++i)
	{
		pthread_create(threads + i, NULL, *threadMapReduce, contexts + i);
	}

	return new JobContext(threads);    // TODO: should threads be allocated with alloc?
}

void waitForJob(JobHandle job)
{
	JobContext jobContext = (JobContext) job;
	while (jobContext.state.stage != REDUCE_STAGE || jobContext.state.percentage < 100)
	{
		std::cout << "Waiting...";    // TODO: Remove this.
	}
	// TODO: to reduce *OVERHEAD*, maybe actually wait()
}

void getJobState(JobHandle job, JobState *state)
{
	*state = ((JobContext) job).state;
}

void closeJobHandle(JobHandle job);