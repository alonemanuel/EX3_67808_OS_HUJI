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
using std::vector;
using std::pair;
typedef struct ThreadContext;

void shuffle(ThreadContext *context);

typedef struct JobContext
{
	vector<ThreadContext> *threads;
	JobState state;

	JobContext(vector<ThreadContext> *_threads) : threads(_threads)
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
	vector<pair<K2 *, V2 *>> *interVec;
	int interVec_size;
	std::atomic<int> *atomicCounter;
	std::vector<std::pair<K3 *, V3 *>> outputVec;
	const MapReduceClient *client;
	Barrier *barrier;

	ThreadContext(JobContext *_jobContext, int _threadNum, std::vector<std::pair<K1 *, V1 *>>
	_inputVec, vector<pair<K2 *, V2 *>> *_interVec, int _interVec_size,
				  std::atomic<int> *_atomicCounter, std::vector<std::pair<K3 *, V3 *>>
				  _outputVec, const MapReduceClient *_client, Barrier *_barrier) :
			jobContext(_jobContext),
			threadNum(_threadNum), inputVec(
			std::move(_inputVec)),
			interVec(_interVec),
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

	cout << LOG_PREFIX << "Putting pairs into interVec" << endl;
	argP.jobContext->state.stage =
			argP.jobContext->state.stage == UNDEFINED_STAGE ? MAP_STAGE : UNDEFINED_STAGE;
	vector<pair<K2 *, V2 *>> *interVec = argP.interVec;
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
		argP.client->map(currPair.first, currPair.second, interVec);
		argP.jobContext->state.percentage = oldValue / (float) argP.inputVec.size() * 100;
	}
	cout << LOG_PREFIX << "Done putting pairs into thread " << argP.threadNum << " which is now of "
																				 "actual size " <<
		 interVec->size() << endl;

	// TODO: Check this:
	std::sort(interVec->begin(), interVec->end());
	cout << LOG_PREFIX << "Potentially done sorting." << endl;
	argP.barrier->barrier(); // waiting for unlock
	cout << LOG_PREFIX << "Thread " << argP.threadNum << " arrived at barrier." << endl;
	// Only the first thread does the shuffle phase.
	if (argP.threadNum == 0)
	{
		shuffle(argP.jobContext);
	}
	argP.jobContext->state.stage =
			argP.jobContext->state.stage == UNDEFINED_STAGE ? MAP_STAGE : UNDEFINED_STAGE;
// TODO: Reduce
}

JobHandle startMapReduceJob(const MapReduceClient &client,
							const InputVec &inputVec, OutputVec &outputVec,
							int multiThreadLevel)
{
	cout << endl << LOG_PREFIX << "Starting job on input of size " << inputVec.size() << endl;
	// atomic counter to be used as input vec index.
	auto *atomic_counter = new std::atomic<int>(0);
	pthread_t threadArr[multiThreadLevel];
	auto *threads = new vector<ThreadContext>();
	auto *jobContext = new JobContext(threads);
	cout << LOG_PREFIX << "Created threads struct with " << multiThreadLevel << " threads" << endl;
	auto *barrier = new Barrier(multiThreadLevel);
	int interSize = (int) inputVec.size() / multiThreadLevel;
	auto *interVec = new vector<pair<K2 *, V2 *>>();
	cout << LOG_PREFIX << "Expected work load of each thread is ~" << interSize << endl;
	int diff = (int) inputVec.size() - (interSize * multiThreadLevel);
	for (int i = 0; i < multiThreadLevel; ++i)
	{
		ThreadContext *context = new ThreadContext(jobContext, i, inputVec, interVec,
												   i == 0 ? interSize + diff : interSize,
												   atomic_counter, outputVec, &client,
												   barrier);
		threads->push_back(*context);    // TODO: Should I pass pointer instead?
		pthread_create(threadArr + i, nullptr, (void *(*)(void *)) threadMapReduce, context);
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

K2 *findMaxK2(vector<ThreadContext> *threads)
{
	// TODO: Check null
	K2 *maxK2 = (threads->front().)->first;
	for (const ThreadContext &thread :*threads)
	{
		K2 *curr = thread.interVec->back().first;
		if (curr > maxK2)
		{
			maxK2 = curr;
		}
	}
	return maxK2;
}

void shuffle(JobContext *context)
{
	// Throw all pairs into a single vector.
	auto newInter = new vector<pair<K2 *, V2 *>>();
	for (ThreadContext tc:*context->threads)
	{
		newInter->insert(newInter->begin(), tc.interVec->begin(), tc.interVec->end());
	}
	std::sort(newInter->begin(), newInter->end());

	vector *reduceQueue = new vector<vector<pair<K2 *, V2 *>>>();
	// Go over all pairs.
	K2 *k2max = newInter->back().first;
	pair<K2 *, V2 *> *currPair = &newInter->back();
	vector<pair<K2 *, V2 *>> *currVec = new vector();
	while (!newInter->empty())
	{
		if (currPair->first != k2max)
		{
			reduceQueue->push_back(currVec);
			currVec = new vector();
			k2max = currPair->first;
		}
		currVec->push_back(*currPair);
		newInter->pop_back();
		currPair = &newInter->back();
	}
}