#include <pthread.h>
#include <atomic>
#include <iostream>
#include <utility>
#include <algorithm>
#include <semaphore.h>
#include "MapReduceFramework.h"
#include "Barrier.h"

using std::cout;
using std::endl;
using std::vector;
using std::pair;

struct JobContext;
struct ThreadContext;

// Comparator.
bool comparePtrToPair(IntermediatePair a, IntermediatePair b)
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
	InputVec inputVec;
	// Intermediate vector.
	vector<IntermediatePair> *interVec;
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
	ThreadContext(JobContext *_jobContext, int _threadNum, const InputVec &_inputVec, sem_t *_sem,
				  std::atomic<int> *_atomicCounter, OutputVec &_outputVec, const MapReduceClient *_client,
				  Barrier *_barrier, pthread_mutex_t *_mutex) :
			jobContext(_jobContext), threadNum(_threadNum), inputVec(_inputVec),
			interVec(new IntermediateVec()), sem(_sem), atomicCounter(_atomicCounter),
			outputVec(&_outputVec), client(_client), barrier(_barrier), mutex(_mutex)
	{}

} ThreadContext;

/**
 * @brief Context of a job.
 */
typedef struct JobContext
{
	// Vector of threads alive within this job.
	vector<ThreadContext *> *threads{};
	pthread_t *threadArr;
	// State of the current job.
	JobState *state;

	// Ctor for a JobContext instance. Receives _threads as pointer.
	JobContext(vector<ThreadContext *> *_threads, pthread_t *_threadArr) : threads(_threads), threadArr(_threadArr)
	{
		// Inits state.
		state = new JobState();
		// Sets state.
		state->stage = UNDEFINED_STAGE;
		state->percentage = 0;
	}

	~JobContext()
	{
		if (!threads->empty())
		{
			delete threads->back()->barrier;
			delete threads->back()->mutex;
			delete threads->back()->atomicCounter;
			delete threads->back()->sem;
		}
		for (ThreadContext *tc:*threads)
		{
			delete tc->interVec;
			delete tc;
		}
		delete threads;
		delete[] threadArr;
		delete state;
	}
} JobContext;


/**
 * @brief Emits pairs into context = intermediate vector.
 */
void emit2(K2 *key, V2 *value, void *context)
{
	auto *interVec = (vector<IntermediatePair> *) context;
	auto p = IntermediatePair(key, value);
	interVec->push_back(p);
}

void emit3(K3 *key, V3 *value, void *context)
{
	auto curr_context = (ThreadContext *) context;
	auto p = OutputPair(key, value);
	curr_context->outputVec->push_back(p);

}

// Mapping
void mapPhase(ThreadContext *context)
{
	context->jobContext->state->stage = MAP_STAGE;
	vector<IntermediatePair> *interVec = context->interVec;
	int oldValue;
	// Use atomic to avoid race conditions.
	while ((unsigned long int) context->atomicCounter->load() < context->inputVec.size())
	{
		oldValue = (*(context->atomicCounter))++;
		InputPair currPair = context->inputVec.at(static_cast<unsigned int>(oldValue));
		// Map each pair.
		context->client->map(currPair.first, currPair.second, interVec);
		// Update percentage.
		context->jobContext->state->percentage = ((oldValue + 1) / (float) context->inputVec.size() * 100);
	}
}

// Sorting.
void sortPhase(ThreadContext *context)
{
	std::sort(context->interVec->begin(), context->interVec->end(), comparePtrToPair);
}

void shufflePhase(vector<IntermediateVec> *reduceQueue, ThreadContext *context)
{
	context->jobContext->state->stage = REDUCE_STAGE;

	// Throw all pairs into a single vector.
	IntermediateVec newInter;
	for (ThreadContext *tc:*context->jobContext->threads)
	{
		newInter.insert(newInter.begin(), tc->interVec->begin(), tc->interVec->end());
	}
	std::sort(newInter.begin(), newInter.end(), comparePtrToPair);

	// Go over all pairs.
	K2 *k2max = newInter.back().first;
	IntermediatePair *currPair = &newInter.back();
	IntermediateVec currVec;
	while (!newInter.empty())
	{
		if (*currPair->first < *k2max)
		{
			if (pthread_mutex_lock(context->mutex) != 0)
			{
				fprintf(stderr, "Shuffle: error on pthread_mutex_lock");
				exit(1);
			}
			reduceQueue->push_back(currVec);
			sem_post(context->sem);
			if (pthread_mutex_unlock(context->mutex) != 0)
			{
				fprintf(stderr, "Shuffle: error on pthread_mutex_unlock");
				exit(1);
			}
			currVec = IntermediateVec();
			k2max = currPair->first;
		}
		currVec.push_back(*currPair);
		newInter.pop_back();
		currPair = &newInter.back();
	}


	if (pthread_mutex_lock(context->mutex) != 0)
	{
		fprintf(stderr, "Shuffle: error on pthread_mutex_lock");
		exit(1);
	}
	if (!currVec.empty())
	{
		reduceQueue->push_back(currVec);
		sem_post(context->sem);

	}
	if (pthread_mutex_unlock(context->mutex) != 0)
	{
		fprintf(stderr, "Shuffle: error on pthread_mutex_unlock");
		exit(1);
	}

}

// Reducing
void reducePhase(vector<IntermediateVec> *reduceQueue, int reduceSize, ThreadContext *context)
{
	while (!reduceQueue->empty())
	{
		sem_wait(context->sem);

		if (pthread_mutex_lock(context->mutex) != 0)
		{
			fprintf(stderr, "Reduce: error on pthread_mutex_lock");
			exit(1);
		}

		if (!reduceQueue->empty())
		{
			context->client->reduce(&reduceQueue->back(), context);
			reduceQueue->pop_back();
			context->jobContext->state->percentage = (reduceSize - reduceQueue->size()) / (float) reduceSize * 100;
		}
		else
		{
			sem_post(context->sem);
		}

		if (pthread_mutex_unlock(context->mutex) != 0)
		{
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
	context->barrier->barrier(); // waiting for unlock.
	auto *reduceQueue = new vector<IntermediateVec>();
	if (context->threadNum == 0)
	{
		shufflePhase(reduceQueue, context);
	}
	reducePhase(reduceQueue, reduceQueue->size(), context);
	delete reduceQueue;
}

JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec,
							int multiThreadLevel)
{
	auto *sem = new sem_t();

	// atomic counter to be used as input vec index.
	auto *atomic_counter = new std::atomic<int>(0);
	auto *threads = new vector<ThreadContext *>();
	auto *threadArr = new pthread_t[multiThreadLevel];
	auto *jobContext = new JobContext(threads, threadArr);
	auto *barrier = new Barrier(multiThreadLevel);
	auto *mutex = new pthread_mutex_t(PTHREAD_MUTEX_INITIALIZER);
	for (int i = 0; i < multiThreadLevel; ++i)
	{
		ThreadContext *context = new ThreadContext(jobContext, i, inputVec, sem,
												   atomic_counter, outputVec, &client,
												   barrier, mutex);
		threads->push_back(context);
		pthread_create(threadArr + i, nullptr, (void *(*)(void *)) threadMapReduce, context);
	}
	return jobContext;
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
	for (int i = 0; i < (int)jobContext->threads->size(); ++i)
	{
		pthread_join(jobContext->threadArr[i], nullptr);
	}
	delete jobContext;
}
