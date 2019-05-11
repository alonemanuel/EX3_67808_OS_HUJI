#include <pthread.h>
#include <atomic>
#include <iostream>
#include <utility>
#include <algorithm>
#include <semaphore.h>
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

bool comparePtrToPair(pair<K2 *, V2 *> a, pair<K2 *, V2 *> b)
{ return a.first->operator<(*b.first); }

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
	// Semaphore.
	sem_t *sem;
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
	_inputVec, sem_t *_sem, std::atomic<int> *_atomicCounter, std::vector<std::pair<K3 *, V3 *>>
				  _outputVec,
				  const MapReduceClient *_client, Barrier *_barrier) :
			jobContext(_jobContext), threadNum(_threadNum), inputVec(std::move(_inputVec)),
			interVec(new vector<pair<K2 *, V2 *>>()), sem(_sem), atomicCounter(_atomicCounter),
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

void mapPhase(ThreadContext *context)
{
	cout << LOG_PREFIX << "Starting map phase for thread " << context->threadNum << endl;
	context->jobContext->state->stage = MAP_STAGE;
	vector<pair<K2 *, V2 *>> *interVec = context->interVec;
	// Use atomic to avoid race conditions.
	while (context->atomicCounter->load() < context->inputVec.size())
	{
		int oldValue = (*(context->atomicCounter))++;
		std::pair<K1 *, V1 *> currPair = context->inputVec.at(oldValue);
		// Map each pair.
		context->client->map(currPair.first, currPair.second, interVec);
		// Update percentage.
		context->jobContext->state->percentage = oldValue / (float) context->inputVec.size() * 100;
	}
}

void sortPhase(ThreadContext *context)
{
	std::sort(context->interVec->begin(), context->interVec->end(), comparePtrToPair);
}

void shufflePhase(JobContext *context, vector<vector<pair<K2 *, V2 *>>> *reduceQueue, sem_t *sem)
{
	cout << LOG_PREFIX << "Starting shuffle phase." << endl;
	context->state->stage = REDUCE_STAGE;

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
		if (*currPair->first < *k2max)
		{
			reduceQueue->push_back(*currVec);
			sem_post(sem);
			currVec = new vector<pair<K2 *, V2 *>>();
			k2max = currPair->first;
		}
		currVec->push_back(*currPair);
		newInter->pop_back();
		currPair = &newInter->back();
	}
}

void reducePhase(ThreadContext *context, vector<vector<pair<K2 *, V2 *>>> *reduceQueue)
{

	while (!reduceQueue->empty())
	{
		sem_wait(context->sem);
	}


}

/**
 * @brief The main function of each thread.
 */
void threadMapReduce(ThreadContext *context)
{
	mapPhase(context);
	sortPhase(context);
	context->barrier->barrier(); // waiting for unlock. TODO: Check this.
	auto *reduceQueue = new vector<vector<pair<K2 *, V2 *>>>();
	if (context->threadNum == 0)
	{
		shufflePhase(context->jobContext, reduceQueue, context->sem);
	}
	reducePhase(context, reduceQueue);
}

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec,
				  int multiThreadLevel)
{
	cout << endl << LOG_PREFIX << "Starting job on input of size " << inputVec.size() << endl;
	// TODO: should new be called on the semaphore?
	sem_t *sem=new sem_t();
	sem_init(sem, multiThreadLevel, 0);


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
		ThreadContext *context = new ThreadContext(jobContext, i, inputVec, sem,
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
