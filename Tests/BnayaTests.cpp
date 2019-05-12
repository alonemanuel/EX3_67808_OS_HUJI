#include <iostream>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"

#define RUN_TEST(functionMame) if(functionMame()){ \
    std::cout << #functionMame << " OK" << std::endl;\
   }\
       else{\
        std::cout << #functionMame << " FAILED" << std::endl;\
       }


class AKey : public k1Base, public k2Base, public k3Base
{
    public:
        AKey(int i): m_i(i)
        {
        }
         
        int getInt() const
        {
            return m_i;
        } 

       
        virtual bool operator<(const k2Base &other) const 
        {
            return m_i < dynamic_cast<const AKey&>(other).m_i;
        }
        virtual bool operator<(const k3Base &other) const 
        {
            return m_i < dynamic_cast<const AKey&>(other).m_i;
        }
        virtual bool operator<(const k1Base &other) const 
        {
            return m_i < dynamic_cast<const AKey&>(other).m_i;
        }
    private:
        int m_i;
};

class SameKey :  public k1Base, public k2Base, public k3Base
{
    public: 
    SameKey(int i): m_i(i)
    {
    }

    virtual bool operator<(const k2Base &other) const
    {
        (void)other;
        return false;
    }
    
    virtual bool operator<(const k3Base &other) const
    {
        (void)other;
        return false;
    }
                 
    virtual bool operator<(const k1Base &other) const
    {
        (void)other;
        return false;
    }
    private:
        int m_i;
};

class IntVal: public v3Base
{
    public:
        IntVal(int i):m_i(i)
        {
        }
        int getInt() const
        {
           return m_i;
        } 
    private:
            int m_i;
};

class MyMapReduce: public MapReduceBase
{
    void Map(const k1Base *const key, const v1Base *const val) const
    {
        Emit2(dynamic_cast<k2Base*>(const_cast<k1Base*>(key)), nullptr);
        (void)val;
    }
    
    void Reduce(const k2Base *const key, const V2_LIST &vals) const
    {
        IntVal* val = new IntVal(vals.size());
        Emit3(dynamic_cast<k3Base*>(const_cast<k2Base*>(key)), val);
    }
};

bool eachInputHandleOnce()
{
    bool testResult = true;
    MyMapReduce mapReduce;
    IN_ITEMS_LIST list;
    for (int i = 0; i< 100000;i++)
    {
        list.push_back(IN_ITEM(new AKey(i),nullptr));
    }
    OUT_ITEMS_LIST result = runMapReduceFramework(mapReduce, list, 100);
    

    for( OUT_ITEM& item: result)
    {
         if (1 != dynamic_cast<IntVal*>(item.second)->getInt())
         {
             testResult = false;
         }
    } 

    for (IN_ITEM& item: list)
    {
        delete item.first;
    }

    for( OUT_ITEM& item: result)
    { 
        delete item.second;
    }

    return testResult;
}

bool outputIsOrdered()
{
    bool testResult = true;
    MyMapReduce mapReduce;
    IN_ITEMS_LIST list;
    for (int i = 100000; i>= 0;i--)
    {
        list.push_back(IN_ITEM(new AKey(i),nullptr));
    }
    OUT_ITEMS_LIST result = runMapReduceFramework(mapReduce, list, 100);
    
    int i = 0;
    for( OUT_ITEM& item: result)
    {
         if (i != dynamic_cast<const AKey*>(item.first)->getInt())
         {
             testResult = false;
         }
        i++;
    }

    for (IN_ITEM& item: list)
    {
        delete item.first;
    }

    for( OUT_ITEM& item: result)
    { 
        delete item.second;
    }
    
    return testResult;
}


bool useOperatorForCompare()
{
    bool testResult = true;
    MyMapReduce mapReduce;
    IN_ITEMS_LIST list;
    for (int i = 0; i< 100000;i++)
    {
        list.push_back(IN_ITEM(new SameKey(i),nullptr));
    }
    OUT_ITEMS_LIST result = runMapReduceFramework(mapReduce, list, 100);
    
    if (1 != result.size())
    {
        testResult = false;
    }

    for( OUT_ITEM& item: result)
    {
         if (100000 != dynamic_cast<IntVal*>(item.second)->getInt())
         {
             testResult = false;
         }
    } 
    
    for (IN_ITEM& item: list)
    {
        delete item.first;
    }

    for( OUT_ITEM& item: result)
    { 
        delete item.second;
    }
    return testResult;
}


int main()
{
    RUN_TEST(eachInputHandleOnce);
    RUN_TEST(outputIsOrdered);
    RUN_TEST(useOperatorForCompare);
}
