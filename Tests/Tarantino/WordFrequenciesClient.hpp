#ifndef OS_EX3_WORDFREQUENCIESCLIENT_HPP
#define OS_EX3_WORDFREQUENCIESCLIENT_HPP

#include <fstream>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include "../../MapReduceClient.h"
#include "../../MapReduceFramework.h"

const int SLEEP_US = 20;

class Line : public K1
{
private:
    const std::string line;

public:
    Line(const std::string &line) : line(line)
    {}

    virtual bool operator<(const K1 &other) const
    {
        return this->line < ((Line &) other).line;
    }

    const std::string &getLine()
    {
        return line;
    }
};

class Word : public K2, public K3
{
private:
    const std::string word;
public:
    Word(const std::string &word) : word(word)
    {}

    Word(const Word &other) = default;

    virtual bool operator<(const K2 &other) const
    {
        return this->word < ((Word &) other).word;
    }

    virtual bool operator<(const K3 &other) const
    {
        return this->word < ((Word &) other).word;
    }

    const std::string &getWord()
    {
        return word;
    }
};

class Integer : public V3
{
public:
    int val;

    Integer(int val) : val(val)
    {}
};

class MapReduceWordFrequencies : public MapReduceClient
{
    virtual void map(const K1 *const key, const V1 *const val, void *context) const
    {
        std::stringstream sstream(((Line *) key)->getLine());
        std::string word;
        while (sstream >> word)
        {
            Word *k2 = new Word(word);
            emit2(k2, nullptr, context);
            usleep(SLEEP_US);
        }
        delete key;
        delete val;
    }

    virtual void reduce(const IntermediateVec *pairs, void *context) const
    {
        Word *k3 = new Word(((Word &) *(pairs->front().first)));
        auto *frequency = new Integer(static_cast<int>(pairs->size()));
        emit3(k3, frequency, context);
        usleep(SLEEP_US * 5);
        for (auto pair: *pairs)
        {
            delete pair.first;
            delete pair.second;
        }
        // delete pairs;
    }
};

#endif //OS_EX3_WORDFREQUENCIESCLIENT_HPP
