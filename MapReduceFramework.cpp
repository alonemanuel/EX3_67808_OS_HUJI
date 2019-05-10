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
typedef struct JobContext;

void shuffle(JobContext *context, vector<vector<pair<K2 *, V2 *>>> *reduceQueue);

/**
 * @brief Context of a job.
 */
typedef struct JobContext
{
	// Vector of threads alive within this job.
	vector<ThreadContext> *threads;
	// State of the current job.
	JobState *state;

	// Ctor for a JobContext instance. Receives _threads as pointer.
	JobContext(vector<ThreadContext> *_threads) : threads(_threads)
	{
		// Inits state.
		state = new JobState();
		// Sets state.
		state->stage = UNDEFINED_STAGE;
		state->percentage = 0;
	}
} JobContext;

/**
 * @brief Context of a thread.
 */
typedef struct ThreadContext
{
	// Job that spawned the thread.
	JobContext *jobContext;
	// Number of thread given upon initialization.
	int threadNum;
	// Input vector given by client.
	vector<pair<K1 *, V1 *>> inputVec;
	// Intermediate vector.
	vector<pair<K2 *, V2 *>> *interVec;
	// Atomic counter.
	std::atomic<int> *atomicCounter;
	// Output vector.
	vector<pair<K3 *, V3 *>> outputVec;
	// Client.
	const MapReduceClient *client;
	// Barrier.
	Barrier *barrier;

	// Ctor.
	ThreadContext(JobContext *_jobContext, int _threadNum, std::vector<std::pair<K1 *, V1 *>>
	_inputVec, std::atomic<int> *_atomicCounter, std::vector<std::pair<K3 *, V3 *>> _outputVec,
				  const MapReduceClient *_client, Barrier *_barrier) :
			jobContext(_jobContext), threadNum(_threadNum), inputVec(std::move(_inputVec)),
			interVec(new vector<pair<K2 *, V2 *>>()), atomicCounter(_atomicCounter),
			outputVec(std::move(_outputVec)), client(_client), barrier(_barrier)
	{}

} ThreadContext;

/**
 * @brief Emits pairs into context = intermediate vector.
 */
void emit2(K2 *key, V2 *value, void *context)
{
	auto *contextP = (vector<pair<K2 *, V2 *>> *) context;    // contextP = interVec
	contextP->push_back(*(new pair<K2 *, V2 *>(key, value)));
}

void emit3(K3 *key, V3 *value, void *context)
{

}

/**
 * @brief The main function of each thread.
 */
void threadMapReduce(void *arg)
{
	// Unpack arg.
	auto argP = *((ThreadContext *) arg);
	cout << LOG_PREFIX << "Putting pairs into interVec" << endl;
	argP.jobContext->state->stage = MAP_STAGE;
	vector<pair<K2 *, V2 *>> *interVec = argP.interVec;
	// Use atomic to avoid race conditions.
	while (argP.atomicCounter->load() < argP.inputVec.size())
	{
		int oldValue = (*(argP.atomicCounter))++;
		// TODO: Atomic counter is unused.
		std::pair<K1 *, V1 *> currPair = argP.inputVec.at(oldValue);
		//Debugging every Xth
		if (oldValue % 1000 == 0)
		{
			cout << LOG_PREFIX << "Mapping pair " << oldValue << " to thread " << argP.threadNum
				 << endl;
		}
		// Map each pair.
		argP.client->map(currPair.first, currPair.second, interVec);
		// Update percentage.
		argP.jobContext->state->percentage = oldValue / (float) argP.inputVec.size() * 100;
	}
	cout << LOG_PREFIX << "Done putting pairs into thread " << argP.threadNum << " which is now of "
																				 "actual size "
		 << interVec->size() << endl;

	// TODO: Check this:
	std::sort(interVec->begin(), interVec->end());
	cout << LOG_PREFIX << "Potentially done sorting." << endl;
	argP.barrier->barrier(); // waiting for unlock
	cout << LOG_PREFIX << "Thread " << argP.threadNum << " arrived at barrier." << endl;
	// Only the first thread does the shuffle phase.
	auto *reduceQueue = new vector<vector<pair<K2 *, V2 *>>>();
	if (argP.threadNum == 0)
	{
		shuffle(argP.jobContext, reduceQueue);
		cout << LOG_PREFIX << "Thread 0 done shuffling. reduceQueue has " << reduceQueue->size()
			 << " elements." << endl;    // TODO: Apparently skipping one element.
	}
	argP.jobContext->state->stage = REDUCE_STAGE;
// TODO: Reduce
}

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec,
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
	cout << LOG_PREFIX << "Expected work load of each thread is ~"
		 << (int) inputVec.size() / multiThreadLevel << endl;
	for (int i = 0; i < multiThreadLevel; ++i)
	{
		ThreadContext *context = new ThreadContext(jobContext, i, inputVec,
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
	while (jobContext->state->stage != REDUCE_STAGE || jobContext->state->percentage < 100)
	{
		std::cout << "Waiting...";    // TODO: Remove this.
	}
	// TODO: to reduce *OVERHEAD*, maybe actually wait()
}

void getJobState(JobHandle job, JobState *state)
{
	JobContext *jc = (JobContext *) job;
	*state = *jc->state;
}

void closeJobHandle(JobHandle job)
{}

bool comparePtrToPair(pair<K2 *, V2 *> a, pair<K2 *, V2 *> b)
{ return a.first->operator<(*b.first); }

void shuffle(JobContext *context, vector<vector<pair<K2 *, V2 *>>> *reduceQueue)
{

	// Throw all pairs into a single vector.
	auto *newInter = new vector<pair<K2 *, V2 *>>();
	for (ThreadContext tc:*context->threads)
	{
		newInter->insert(newInter->begin(), tc.interVec->begin(), tc.interVec->end());
	}
	std::sort(newInter->begin(), newInter->end(), comparePtrToPair);

	// Go over all pairs.
	K2 *k2max = newInter->back().first;
	pair<K2 *, V2 *> *currPair = &newInter->back();
	auto *currVec = new vector<pair<K2 *, V2 *>>();
	while (!newInter->empty())
	{
		if (*currPair->first<*k2max)
		{
			reduceQueue->push_back(*currVec);
			currVec = new vector<pair<K2 *, V2 *>>();
			k2max = currPair->first;
		}
		currVec->push_back(*currPair);
		newInter->pop_back();
		currPair = &newInter->back();
	}
}