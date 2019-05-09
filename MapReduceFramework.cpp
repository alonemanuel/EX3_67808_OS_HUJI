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
	JobContext *jobContext;
	int threadNum;
	std::vector<std::pair<K1 *, V1 *>> inputVec;
	int interVec_size;
	std::atomic<int> *atomicCounter;
	std::vector<std::pair<K3 *, V3 *>> outputVec;
	const MapReduceClient *client;
	Barrier *barrier;

	ThreadContext(JobContext *_jobContext, int _threadNum, std::vector<std::pair<K1 *, V1 *>>
	_inputVec, int
				  _interVec_size,
				  std::atomic<int> *_atomicCounter, std::vector<std::pair<K3 *, V3 *>>
				  _outputVec, const MapReduceClient *_client, Barrier *_barrier) :
			jobContext(_jobContext),
			threadNum(_threadNum), inputVec(
			std::move(_inputVec)),
			interVec_size(
					_interVec_size),
			atomicCounter(
					_atomicCounter),
			outputVec(
					std::move(
							_outputVec)),
			client(_client),
			barrier(_barrier)
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

//// TODO: Debugging only
//void printVector(std::vector<std::pair<K2 *, V2 *>> toPrint)
//{
//	for (auto entry : toPrint)
//	{
//		std::cout << "Key: " << ((Word *) entry.first)->getWord() << std::endl;
//	}
//}

void threadMapReduce(void *arg)
{

	auto argP = *((ThreadContext *) arg);

	auto *inter = new std::vector<std::pair<K2 *, V2 *>>();
	cout << LOG_PREFIX << "Putting pairs into interVec" << endl;
	argP.jobContext->state.stage = MAP_STAGE;
	while (argP.atomicCounter->load() < argP.inputVec.size())
		//	for (int i = 0; i < argP.interVec_size; ++i)
	{
		int oldValue = (*(argP.atomicCounter))++;    // This could cause index out of bounds
		// TODO: Atomic counter is unused.
		std::pair<K1 *, V1 *> currPair = argP.inputVec.at(oldValue);
		//Debugging every Xth
		if (oldValue % 1000 == 0)
		{
			cout << LOG_PREFIX << "Mapping pair " << oldValue << " to thread " << argP.threadNum
				 << endl;
		}
		argP.client->map(currPair.first, currPair.second, inter);
		argP.jobContext->state.percentage = oldValue / (float) argP.inputVec.size()*100;
	}
	cout << LOG_PREFIX << "Done putting pairs into thread " << argP.threadNum << " which is now of "
																				 "actual size " <<
		 inter->size() << endl;

	// TODO: Check this:
	std::sort(inter->begin(), inter->end());
	cout << LOG_PREFIX << "Potentially done sorting." << endl;
	argP.barrier->barrier(); // waiting for unlock
	cout << LOG_PREFIX << "Thread " << argP.threadNum << " arrived at barrier." << endl;

}

JobHandle startMapReduceJob(const MapReduceClient &client,
							const InputVec &inputVec, OutputVec &outputVec,
							int multiThreadLevel)
{
	cout << endl << LOG_PREFIX << "Starting job on input of size " << inputVec.size() << endl;
	std::atomic<int> *atomic_counter = new std::atomic<int>(0);    // atomic counter to be used
// as input vec index.

	pthread_t threads[multiThreadLevel];    // TODO: Should we have multiple threadContexts or one?
	cout << LOG_PREFIX << "Created threads structure with " << multiThreadLevel << " threads"
		 << endl;
	JobContext *jobContext = new JobContext(threads);
	Barrier *barrier = new Barrier(multiThreadLevel);
	int interSize = (int) inputVec.size() / multiThreadLevel;
	cout << LOG_PREFIX << "Expected work load of each thread is ~" << interSize << endl;
	int diff = (int) inputVec.size() - (interSize * multiThreadLevel);

	for (int i = 0; i < multiThreadLevel; ++i)
	{
		ThreadContext *context = new ThreadContext(jobContext, i, inputVec,
												   i == 0 ? interSize + diff : interSize,
												   atomic_counter, outputVec, &client,
												   barrier);
		pthread_create(threads + i, nullptr, (void *(*)(void *)) threadMapReduce, context);
		cout << LOG_PREFIX << "Created context and thread " << i << endl;
	}

	cout << LOG_PREFIX << "Ended job" << endl << endl;
	return jobContext;    // TODO: should threads be allocated with alloc?
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