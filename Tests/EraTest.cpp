#include <iostream>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"


using namespace std;

// All the user implemented classes
class k1Derived: public K1
{
    public:
        int myNum;
    k1Derived(int x): myNum(x){};
    ~k1Derived(){};
    bool operator<(const K1 &other) const
    {
        return ((const k1Derived&) other).myNum < myNum;
    }
};

class v1Derived: public V1
{
    ~v1Derived();
};
class k2Derived: public K2
{
    public:
        int myNum;
    k2Derived(int x): myNum(x){};
    ~k2Derived(){}
    bool operator<(const K2 &other) const
    {
        return  myNum < ((const k2Derived&) other).myNum;
    }
};

class v2Derived: public V2
{
public:
    int myNum;
    v2Derived(int x): myNum(x){};
    ~v2Derived(){}
};

class k3Derived: public K3
{
public:
    int myNum;
    k3Derived(int x): myNum(x){};
    ~k3Derived(){}
    bool operator<(const K3 &other) const
    {
        return myNum<((const k3Derived&) other).myNum;
    }

};

class v3Derived: public V3
{
public:
    int myNum;
    v3Derived(int x): myNum(x){};
    ~v3Derived(){}

};

class derivedMapReduce: public MapReduceBase {
    void Map(const K1 *const key, const V1 *const val) const override {
        k2Derived* convertedK2 = new k2Derived(((const k1Derived *const) key)->myNum);
        v2Derived* newV2 = new v2Derived(1);
        emit2((K2 *) convertedK2, (V2 *) newV2);
    }

    void Reduce(const K2 *const key, const IntermediateVec &vals) const override {
        k3Derived* convertedK3 = new k3Derived(((const k2Derived *const) key)->myNum);
        v3Derived* newV3 = new v3Derived((int) vals.size());
        emit3((K3 *) convertedK3, (V3 *) newV3);
    }
};


int NUM_THREADS = 20;
int INNER_LOOP = 700;

int NUM_START_OBJECTS = (INNER_LOOP*(INNER_LOOP-1))/2;
int NUM_FINAL_CONTAINERS = INNER_LOOP-1;

int main() {
    derivedMapReduce dmp = derivedMapReduce();
    InputVec iil = InputVec();
    for (int i = 0; i < INNER_LOOP-1; ++i)
    {
        for (int j=i+1; j < INNER_LOOP; ++j)
        {
            k1Derived* currKey =  new k1Derived(j);
            InputPair pair =  InputPair(currKey, nullptr);
            iil.push_back(pair);
        }
    }
    OutputVec oil = runMapReduceFramework( dmp,  iil, NUM_THREADS);
    for(auto it= iil.begin(); it != iil.end(); it++)
    {
        delete (*it).first;
        delete (*it).second;
    }
    for(auto it = oil.begin(); it != oil.end(); it++)
    {
        std::cout << ((k3Derived*)(it->first))->myNum << ": " << ((v3Derived*)(it->second))->myNum << std::endl;
        delete it->first;
        delete it->second;

    }
    std::cout << "AMOUNT OF BUCKETS" << oil.size() << std::endl;
    return 0;
}