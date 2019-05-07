#include <pthread.h>
#include <atomic>
#include <iostream>
#include <utility>
#include <algorithm>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "Barrier.h"
#include "Tests/Tarantino/WordFrequenciesClient.hpp"

using std::cout;
using std::endl;

typedef struct JobContext
{
	pthread_t *threads;
	JobState state;

	JobContext(pthread_t *_threads) : threads(_threads)
	{
		state.stage = MAP_STAGE;
		state.percentage = 0;
	}
} JobContext;


typedef struct ThreadContext
{
	std::vector<std::pair<K1 *, V1 *>> inputVec;
	int interVec_size;
	std::atomic<int> *atomicCounter;
	std::vector<std::pair<K3 *, V3 *>> outputVec;
	const MapReduceClient *client;
	Barrier *barrier;

	ThreadContext(std::vector<std::pair<K1 *, V1 *>> _inputVec, int _interVec_size,
				  std::atomic<int> *_atomicCounter, std::vector<std::pair<K3 *, V3 *>>
				  _outputVec, const MapReduceClient *_client, Barrier *_barrier) : inputVec(std::move(_inputVec)),
																				   interVec_size(_interVec_size),
																				   atomicCounter(_atomicCounter),
																				   outputVec(std::move(_outputVec)),
																				   client(_client), barrier(_barrier)
	{}

} ThreadContext;

// context = intermediate vector.
void emit2(K2 *key, V2 *value, void *context)
{
	auto *contextP = (std::vector<std::pair<K2 *, V2 *>> *) context;    // contextP = interVec
	contextP->push_back(*(new std::pair<K2 *, V2 *>(key, value)));
}

void emit3(K3 *key, V3 *value, void *context)
{

}


// TODO: Debugging only
void printVector(std::vector<std::pair<K2 *, V2 *>> toPrint)
{
	for (auto entry : toPrint)
	{
		std::cout << "Key: " << ((Word*)entry.first)->getWord()<< std::endl;
	}
}

void threadMapReduce(void *arg)
{
	auto argP = *((ThreadContext *) arg);

	auto *inter = new std::vector<std::pair<K2 *, V2 *>>();

	for (int i = 0; i < argP.interVec_size; ++i)
	{
		(*(argP.atomicCounter))++;    // to avoid race conditions. TODO: mutex?
		signed int oldVal = argP.atomicCounter->load() -1;
		std::pair<K1 *, V1 *> currPair = argP.inputVec.at((unsigned long) oldVal);
		argP.client->map(currPair.first, currPair.second, inter);
//		printVector(*inter);
	}

	std::sort(inter->begin(), inter->end());
	argP.barrier->barrier(); // waiting for unlock


}

JobHandle startMapReduceJob(const MapReduceClient &client,
							const InputVec &inputVec, OutputVec &outputVec,
							int multiThreadLevel)
{
	std::atomic<int> *atomic_counter = new std::atomic<int>(0);    // atomic counter to be used as input vec index.

//	std::atomic<int> atomic_counter(0);    // atomic counter to be used as input vec index.
	pthread_t threads[multiThreadLevel];    // TODO: Should we have multiple threadContexts or one?
	Barrier *barrier = new Barrier(multiThreadLevel);
	for (int i = 0; i < multiThreadLevel; ++i)
	{
		ThreadContext *context = new ThreadContext(inputVec, (int) (inputVec.size() / multiThreadLevel),
												   atomic_counter, outputVec, &client, barrier);
		pthread_create(threads + i, nullptr, (void *(*)(void *)) threadMapReduce, context);
	}

	return new JobContext(threads);    // TODO: should threads be allocated with alloc?
}

void waitForJob(JobHandle job)
{
	auto *jobContext = (JobContext *) job;
	while (jobContext->state.stage != REDUCE_STAGE || jobContext->state.percentage < 100)
	{
		std::cout << "Waiting...";    // TODO: Remove this.
	}
	// TODO: to reduce *OVERHEAD*, maybe actually wait()
}

void getJobState(JobHandle job, JobState *state)
{
	JobContext *jc = (JobContext *) job;
	*state = jc->state;
}

void closeJobHandle(JobHandle job)
{}