#include "MapReduceFramework.h"
#include <cstdio>
#include <string>
#include <array>
#include <fstream>

class VString : public V1 {
public:
    VString(std::string content) : content(content) {}
    std::string content;
};

class KChar : public K2, public K3 {
public:
    KChar(char c) : c(c) {}

    virtual bool operator<(const K2 &other) const {
        return c < static_cast<const KChar &>(other).c;
    }

    virtual bool operator<(const K3 &other) const {
        return c < static_cast<const KChar &>(other).c;
    }

    char c;
};

class VCount : public V2, public V3 {
public:
    VCount(int count) : count(count) {}

    int count;
};


class CounterClient : public MapReduceClient {
public:
    void map(const K1 *key, const V1 *value, void *context) const {
        std::array<int, 256> counts;
        counts.fill(0);
        for (const char &c : static_cast<const VString *>(value)->content) {
            counts[(unsigned char) c]++;
        }

        for (int i = 0; i < 256; ++i) {
            if (counts[i] == 0)
                continue;

            KChar *k2 = new KChar(i);
            VCount *v2 = new VCount(counts[i]);
            emit2(k2, v2, context);
        }
    }

    virtual void reduce(const IntermediateVec *pairs,
                        void *context) const {
        const char c = static_cast<const KChar *>(pairs->at(0).first)->c;
        int count = 0;
        for (const IntermediatePair &pair: *pairs) {
            count += static_cast<const VCount *>(pair.second)->count;
            delete pair.first;
            delete pair.second;
        }
        KChar *k3 = new KChar(c);
        VCount *v3 = new VCount(count);
        emit3(k3, v3, context);
    }
};

typedef struct args {
    CounterClient *client1;
    InputVec *inputVec1;
    OutputVec *outputVec1;
    int num;
};


void *foo(void *arg) {
    auto *arg1 = (args *) arg;
    CounterClient *client = arg1->client1;
    InputVec *inputVec = arg1->inputVec1;
    OutputVec *outputVec = arg1->outputVec1;
    int num = arg1->num;
    runMapReduceFramework(*client, *inputVec, *outputVec, num);
    return nullptr;
};

int main(int argc, char **argv) {
    int files_num = (int) *(argv[1]) - 48;
    pthread_t tids[files_num];
    args arguments[files_num];
    CounterClient *client;
    client = new CounterClient();

    for (int i = 0; i < files_num; ++i) {
        InputVec *inputVec;
        inputVec = new InputVec();
        OutputVec *outputVec;
        outputVec = new OutputVec();
        std::string line;
        std::ifstream input_file1(("/cs/usr/shaked.weitz/ClionProjects/ex3/text_" + std::to_string(i + 1)));
        while (std::getline(input_file1, line)) {
            VString *s = new VString(line);
            inputVec->push_back({nullptr, s});
        }
        arguments[i] = {client, inputVec, outputVec, 100};
        pthread_create(&(tids[i]), NULL, foo, (void *) &(arguments[i]));

    }


//    CounterClient *client2;
//    client2 = new CounterClient();
//    InputVec *inputVec2;
//    inputVec2 = new InputVec();
//    OutputVec *outputVec2;
//    outputVec2 = new OutputVec();
//
//    std::string line;
//    std::ifstream input_file1("/home/netanel/Documents/HUJI/OS/ex3/text_gen/text_1");
//    std::ifstream input_file2("/home/netanel/Documents/HUJI/OS/ex3/text_gen/text_2");
//    inputVec.re*(arguments[k].outputVec1)serve(10000);

//    while (std::getline(input_file2, line)) {
//        VString *s = new VString(line);
//        inputVec2->push_back({nullptr, s});
//    }


//    args arg2 = {client2, inputVec2, outputVec2, 1};
//    pthread_create(&tid2, NULL, foo, (void *) &arg2);
//    runMapReduceFramework(client, inputVec, outputVec, 1);
    for (int j = 0; j < files_num; ++j) {
        pthread_join(tids[j], nullptr);

    }
//    pthread_join(tid2, nullptr);
    for (int k = 0; k < files_num; ++k) {
        printf("File %d:\n", k + 1);
        for (OutputPair &pair: *(arguments[k].outputVec1)) {
            char c = ((const KChar *) pair.first)->c;
            int count = ((const VCount *) pair.second)->count;
            printf("The character %c appeared %d time%s\n",
                   c, count, count > 1 ? "s" : "");
            delete pair.first;
            delete pair.second;
        }
        delete (arguments[k].outputVec1);
        for (auto &str: *(arguments[k].inputVec1)){
            delete str.second;
        }
        delete arguments[k].inputVec1;
    }

    delete client;


//    printf("File 2:\n");
//    for (OutputPair &pair: *outputVec2) {
//        char c = ((const KChar *) pair.first)->c;
//        int count = ((const VCount *) pair.second)->count;
//        printf("The character %c appeared %d time%s\n",
//               c, count, count > 1 ? "s" : "");
//        delete pair.first;
//        delete pair.second;
//    }

    return 0;
}

