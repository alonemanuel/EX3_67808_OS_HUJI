// ------------------------------ includes ------------------------------
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <stdio.h>
#include <signal.h>
#include <sys/time.h>
#include <setjmp.h>
#include <unistd.h>
#include "string.h"
#include <unistd.h>
#include <queue>
#include <iostream>
#include <dirent.h>
#include <sys/stat.h>
#include <iostream>
#include <fstream>
// -------------------------- using definitions -------------------------
using namespace std;
// -------------------------- definitions -------------------------------
typedef std::list<v2Base *> V2_LIST;

class FileToSearch : public k1Base {
public:
	char* file;
	bool operator<(const k1Base &other) const{
		return true;
	}
};

class WordToSearch : public v1Base {
public:
	string word;
};

class Word : public k2Base {
public:
	char* word;
	bool operator<(const k2Base &other) const{
		Word* temp = (Word*) &other;
		Word* temp2 = (Word*) this;
		if(strcmp(temp -> word, temp2 -> word) <= 0){
			return false;
		}
		return true;
	}
};

class ApperanceOfWord : public v2Base {
public:
	int num;
};


class Word2 : public k3Base {
public:
	char* word2;
	bool operator<(const k3Base &other) const{
		Word2* temp = (Word2*) &other;
		Word2* temp2 = (Word2*) this;
		if(strcmp(temp2 -> word2, temp -> word2) < 0){
			return true;
		}
		return false;
	}
};

class ApperanceOfWordList : public v3Base{
public:
	int num;
};

class Count : public MapReduceBase{
public:

	void Map(const k1Base *const key, const v1Base *const val) const{

		DIR *dir;
		struct dirent *ent;
		const char* file = ((FileToSearch*) key) -> file;
		ifstream myfile;
		myfile.open(file);
		string wordToSearch = ((WordToSearch*) val) -> word;
        while (myfile.good())
        {
            string word;
            myfile >> word;

            if ( word == wordToSearch)
            {
	            ApperanceOfWord* apperanceOfWord = new ApperanceOfWord();
	        	apperanceOfWord -> num = 1;
	        	Word* k2 = new Word();
	        	k2 -> word = strdup(wordToSearch.c_str());
		        Emit2(k2,apperanceOfWord);
        	}
        }
        myfile.close();	

	}

    void Reduce(const k2Base *const key, const V2_LIST &vals) const{

		string word = ((Word*)key) -> word;
		int count = 0;
		for (v2Base* val : vals)
		{
			count += ((ApperanceOfWord*)val) -> num;
		}

		Word2* myWord2 = new Word2();
		myWord2 -> word2 = strdup(word.c_str());

		ApperanceOfWordList* apperanceOfWordList = new ApperanceOfWordList();
		apperanceOfWordList -> num = count;

		Emit3(myWord2,apperanceOfWordList);
    }
};


IN_ITEMS_LIST getData(int argc,char *argv[])
{
	IN_ITEMS_LIST res;

	for (int i = 5; i < 10; ++i)
	{
		for (int j = 1; j < 5; ++j)
		{
			FileToSearch* file1 = new FileToSearch();
			file1 -> file = argv[i];
			WordToSearch* word1 = new WordToSearch();
			word1 -> word = argv[j];
			res.push_back(IN_ITEM(file1,word1));
		}
	}
	return res;
}

int main(int argc,char *argv[])
{
	Count count;
	IN_ITEMS_LIST res = getData(argc,argv);
	for(IN_ITEM item : res)
	{
		FileToSearch* a = (FileToSearch*) item.first;
		WordToSearch* b = (WordToSearch*) item.second;
	}

	struct timeval diff, startTV, endTV;

	OUT_ITEMS_LIST finalRes = runMapReduceFramework(count,res,15);

	cout << "***************************************" << endl;
	cout << "uncle sam has a farm,but all his animals escaped and hid" << endl;
	cout << "kids,help uncle sam find his animals in the files" << endl;
	cout << "***************************************" << endl;
	cout << "Recieved: " << endl;

	for(OUT_ITEM temp : finalRes)
	{

		Word2* temp1 = (Word2*) temp.first;
		ApperanceOfWordList* temp2 = (ApperanceOfWordList*) temp.second;
		cout << temp2 -> num << " "<< temp1 -> word2 << endl;
	}

	cout << "***************************************" << endl;
	cout << "Excpected: " << endl;
	cout << "(the order matter,mind you)" << endl;
	cout << "6 Cat " << endl;
	cout << "12 Dog " << endl;
	cout << "10 Fish " << endl;
	cout << "8 Sheep " << endl;

	return 0;
}