/**
 * See README file.
 */

#include "WordFrequenciesClient.hpp"
#include <algorithm>
//#include "../../Tests/Tarantino/scripts"

using std::cout;
using std::endl;
using std::cerr;
using std::sort;
using std::string;
using std::ifstream;
using std::ofstream;
using std::vector;

// TODO: Play with this path to match your directories
const string SCRIPTS_DIR = "/cs/usr/alonemanuel/Year2/Semester2/67808_OS/EX3_67808_OS_HUJI/Tests/Tarantino/scripts/";
const int REPORT_FREQ_MS = 500;

class ScriptJob
{
public:
	JobHandle handle;
	int id;
	string path;
	ifstream ifs;
	ofstream ofs;
	InputVec input;
	OutputVec output;
	JobState state;
	bool initiated;

	ScriptJob(const int id, const string &scriptPath) : id(id), path(scriptPath), input(),
														output(), state{UNDEFINED_STAGE, 0},
														initiated(false)
	{
		// Init files
		string outPath(scriptPath + string("_test_results"));
		ifs = ifstream(scriptPath);
		ofs = ofstream(outPath, ofstream::out);
		if (!ifs.is_open() || !ofs.is_open())
		{
			cerr << "I/O Error occurred - couldn't open input or output file" << endl;
			return;
		}

		// Generate input vector
		string line;
		while (getline(ifs, line))
		{
			Line *k1 = new Line(line);
			input.push_back(InputPair(k1, nullptr));
		}

		initiated = true;
	}

	~ScriptJob()
	{
		ifs.close();
		ofs.close();
	}

	void start(const SampleClient &client, const int &mtLevel)
	{
		if (!initiated)
		{
			return;
		}
		cout << "Starting job on file: " << path << endl;
		handle = startMapReduceJob(client, input, output, mtLevel);
	}

	bool isDone()
	{
		return state.stage == REDUCE_STAGE && state.percentage == 100.0;
	}

	bool reportChange()
	{
		JobState newState;
		getJobState(handle, &newState);
		if (newState.stage != state.stage || newState.percentage != state.percentage)
		{
			printf("Thread [%d]: at stage [%d], [%f]%% \n", id, state.stage, state.percentage);
			state = newState;
			if (isDone())
			{
				printf("Thread [%d] done!\n", id);
				return true;
			}
		}
		return isDone();
	}

	void writeByFrequency()
	{
		cout << "Logging: " << "Output size is: " << output.size() << endl;
		// get length of longest word (so we can write to the file in a nice format)
		unsigned int maxLength = 0;
		for (auto &frequencie : output)
		{
			auto length = static_cast<unsigned int>((*(Word *) frequencie.first).getWord().length());
			maxLength = length > maxLength ? length : maxLength;
		}

		// Sort by frequency in descending order
		sort(output.begin(), output.end(),
			 [](const OutputPair &o1, const OutputPair &o2)
			 {
				 // o1 comes before o2 if the frequency of o1 is higher OR
				 // they have the same frequency and o1 comes before o2 in
				 // lexicographic order

				 if (((Integer *) o1.second)->val < ((Integer *) o2.second)->val)
				 {
					 return false;
				 }

				 return ((Integer *) o1.second)->val > ((Integer *) o2.second)->val
						|| ((Word *) o1.first)->getWord() < ((Word *) o2.first)->getWord();
			 }
			);

		// Writ results to file
		for (auto &pair : output)
		{
			// cout << (*(Word*) it->first).getWord() << endl;
			const string &word = (*(Word *) pair.first).getWord();
			int frequency = ((Integer *) pair.second)->val;
			ofs << word;
			for (unsigned long i = word.length(); i < maxLength + 5; ++i)
			{
				// pad with spaces
				ofs << " ";
			}
			ofs << frequency << endl;
			delete pair.first;
			delete pair.second;
		}
		cout << "Logging: " << "Last pair is: " << ((Word*)output.back().first)->getWord() << endl;
	}
};

int main()
{
	vector<ScriptJob *> jobs;
	vector<string> paths;

	paths.emplace_back(SCRIPTS_DIR + "Inglourious_Basterds");
	paths.emplace_back(SCRIPTS_DIR + "Reservoir_Dogs");
	paths.emplace_back(SCRIPTS_DIR + "Pulp_Fiction");

	// Generate jobs
	jobs.reserve(paths.size());

	int idCount = 1;
	for (const auto &path : paths)
	{
		jobs.push_back(new ScriptJob(idCount++, path));
	}

	// Start jobs
	MapReduceWordFrequencies client;
//    int mtLevel = 1;
	int mtLevel = 3;
	for (auto job :jobs)
	{
		job->start(client, mtLevel);
		mtLevel *= 2;
	}

	// Job 0 will take the longest, report
	while (!jobs[0]->isDone())
	{
		for (auto job : jobs)
		{
			job->reportChange();
		}
		usleep(REPORT_FREQ_MS * 1000);
	}

	// Make sure everyone is dead
	for (auto job :jobs)
	{
		waitForJob(job->handle);
	}

	// Write output
	for (auto job :jobs)
	{
		job->writeByFrequency();
	}

	// Kill
	for (auto job :jobs)
	{
		closeJobHandle(job->handle);
	}

	// Delete pointers
	while (!jobs.empty())
	{
		ScriptJob *job = jobs.back();
		delete job;
		jobs.pop_back();
	}

	cout << endl;
	cout << "If you got here with no memory leaks, it's a good sign." << endl;
	cout << "Don't forget to verify the 'test_results' files with diff." << endl;

}