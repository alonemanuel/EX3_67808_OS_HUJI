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

struct JobContext;
struct ThreadContext;

bool comparePtrToPair(pair<K2 *, V2 *> a, pair<K2 *, V2 *> b)
{ return a.first->operator<(*b.first); }

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
	InputVec *inputVec;
	// Intermediate vector.
	vector<pair<K2 *, V2 *>> *interVec;
	// Semaphore.
	sem_t *sem;
	// Atomic counter.
	std::atomic<int> *atomicCounter;
	// Output vector.
	OutputVec *outputVec;
	// Client.
	const MapReduceClient *client;
	// Barrier.
	Barrier *barrier;
	// Mutex.
	pthread_mutex_t *mutex;

	// Ctor.
	ThreadContext(JobContext *_jobContext, int _threadNum, InputVec * _inputVec, sem_t *_sem,
	        std::atomic<int> *_atomicCounter, OutputVec * _outputVec, const MapReduceClient *_client,
	        Barrier *_barrier, pthread_mutex_t* _mutex) :
			    jobContext(_jobContext), threadNum(_threadNum), inputVec(_inputVec),
			    interVec(new vector<IntermediatePair>()), sem(_sem), atomicCounter(_atomicCounter),
			    outputVec(_outputVec), client(_client), barrier(_barrier), mutex(_mutex)
	{}

} ThreadContext;

/**
 * @brief Context of a job.
 */
typedef struct JobContext
{
    // Vector of threads alive within this job.
    vector<ThreadContext> *threads{};
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

    ~JobContext() {
        if(!threads->empty()){
            delete threads->back().barrier;
            delete threads->back().mutex;
            delete threads->back().atomicCounter;
            delete threads->back().sem;
        }
        delete threads;
    }
} JobContext;


/**
 * @brief Emits pairs into context = intermediate vector.
 */
void emit2(K2 *key, V2 *value, void *context)
{
	auto *interVec = (vector<pair<K2 *, V2 *>> *) context;
	interVec->push_back(*(new pair<K2 *, V2 *>(key, value)));
}

void emit3(K3 *key, V3 *value, void *context)
{
    auto curr_context = (ThreadContext *) context;
    auto p = OutputPair(key, value);
    curr_context->outputVec->push_back(p);
}

void mapPhase(ThreadContext *context)
{
	cout << LOG_PREFIX << "Starting map phase for thread " << context->threadNum << endl;
	context->jobContext->state->stage = MAP_STAGE;
	vector<pair<K2 *, V2 *>> *interVec = context->interVec;
    int oldValue;
	// Use atomic to avoid race conditions.
	while (context->atomicCounter->load() < context->inputVec->size())
	{
		oldValue = (*(context->atomicCounter))++;
		std::pair<K1 *, V1 *> currPair = context->inputVec->at(static_cast<unsigned int>(oldValue));
		// Map each pair.
		context->client->map(currPair.first, currPair.second, interVec);
		// Update percentage.
		context->jobContext->state->percentage = ((oldValue + 1) / (float) context->inputVec->size() * 100);
	}
}

void sortPhase(ThreadContext *context)
{
	std::sort(context->interVec->begin(), context->interVec->end(), comparePtrToPair);
}

void shufflePhase(vector<vector<pair<K2 *, V2 *>>> *reduceQueue, ThreadContext *context)
{
    context->jobContext->state->stage = REDUCE_STAGE;
    cout << LOG_PREFIX << "Starting shuffle phase." << endl;

	// Throw all pairs into a single vector.
	auto *newInter = new vector<pair<K2 *, V2 *>>();
	for (ThreadContext tc:*context->jobContext->threads)
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
            if (pthread_mutex_lock(context->mutex) != 0) {
                fprintf(stderr, "Shuffle: error on pthread_mutex_lock");
                exit(1);
            }
			reduceQueue->push_back(*currVec);
			sem_post(context->sem);
            if (pthread_mutex_unlock(context->mutex) != 0) {
                fprintf(stderr, "Shuffle: error on pthread_mutex_unlock");
                exit(1);
            }
			currVec = new vector<pair<K2 *, V2 *>>();
			k2max = currPair->first;
		}
		currVec->push_back(*currPair);
		newInter->pop_back();
		currPair = &newInter->back();
	}
}

void reducePhase(vector<vector<pair<K2 *, V2 *>>> *reduceQueue, int reduceSize, ThreadContext *context)
{
	while (!reduceQueue->empty())
	{
		sem_wait(context->sem);

        if (pthread_mutex_lock(context->mutex) != 0) {
            fprintf(stderr, "Reduce: error on pthread_mutex_lock");
            exit(1);
        }

        if (!reduceQueue->empty()) {
            context->client->reduce(&reduceQueue->back(), context);
            reduceQueue->pop_back();
            context->jobContext->state->percentage = (reduceSize - reduceQueue->size()) / (float) reduceSize * 100;
        } else {
            sem_post(context->sem);
        }

        if (pthread_mutex_unlock(context->mutex) != 0) {
            fprintf(stderr, "Reduce: error on pthread_mutex_lock");
            exit(1);
        }
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
		shufflePhase(reduceQueue, context);
	}
	reducePhase(reduceQueue, reduceQueue->size(), context);
}

JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec,
				  int multiThreadLevel)
{
	cout << endl << LOG_PREFIX << "Starting job on input of size " << inputVec.size() << endl;
	// TODO: should new be called on the semaphore?
    auto *sem = new sem_t();
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
    auto *mutex = new pthread_mutex_t(PTHREAD_MUTEX_INITIALIZER);
	for (int i = 0; i < multiThreadLevel; ++i)
	{
		ThreadContext *context = new ThreadContext(jobContext, i, inputVec, sem,
												   atomic_counter, outputVec, &client,
												   barrier, mutex);
		threads->push_back(*context);    // TODO: Should I pass pointer instead?
		pthread_create(threadArr + i, nullptr, (void *(*)(void *)) threadMapReduce, context);
		cout << LOG_PREFIX << "Created context and thread " << i << endl;
	}
	return jobContext;    // TODO: should threads be allocated with alloc?
}

void waitForJob(JobHandle job)
{
    auto *jobContext = (JobContext *) job;
    while (jobContext->state->stage != REDUCE_STAGE || jobContext->state->percentage < 100)
	{
    }
}

void getJobState(JobHandle job, JobState *state)
{
    auto *jc = (JobContext *) job;
	*state = *jc->state;
}

void closeJobHandle(JobHandle job)
{
    waitForJob(job);
    auto jobContext = (JobContext *) job;
    delete jobContext;
}
