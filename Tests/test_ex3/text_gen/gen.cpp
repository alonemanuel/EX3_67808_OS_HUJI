#include<iostream>
#include<fstream>
#include<string>
#include<sstream> // for std::stringstream
#include<cstdlib> // for exit()
#include<random>

std::random_device rd;  //Will be used to obtain a seed for the random number engine
std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
std::uniform_int_distribution<> wordLength(1, 60);
std::uniform_int_distribution<> randomChar(97, 122);

std::string getRandomWord(){
		std::stringstream stream;
		//unsigned int letters = wordLength(gen);
		unsigned int letters = 50;
		for (unsigned int i=0; i<letters; ++i){
				stream << char(randomChar(gen));
		}
		return stream.str();
}

int main(int argc, char *argv[]) {
		if (argc <= 2) {
				// On some operating systems, argv[0] can end up as an empty string instead of the program's name.
				// We'll conditionalize our response on whether argv[0] is empty or not.
				std::string programName = "<program name>";
				if (argv[0]){
						programName = std::string(argv[0]);
				}
				std::cout << "Usage: " << programName << " <output_file> <number_of_lines>" << '\n';

				exit(1);
		}

		std::stringstream convert(argv[2]); // set up a stringstream variable named convert, initialized with the input from argv[1]

		int lines;
		if (!(convert >> lines)) // do the conversion
				lines = 1000; // if conversion fails, set myint to a default value

		std::ofstream o(argv[1]);

		for (unsigned int i=1;i<lines;++i){
				o << getRandomWord() << "\n";
		}
		o << getRandomWord();

		return 0;
}

