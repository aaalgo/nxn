#include <limits>
#include <iostream>
#include <fstream>
#include <vector>
#include <boost/timer.hpp>
#include <boost/foreach.hpp>
#include <boost/program_options.hpp>
#include <nxn.h>
#include <nxn++.h>
#include <lshkit/matrix.h>
#include <lshkit/matrix-io.h>
#include "knng.h"

using namespace std;
using namespace nxn;
using namespace knng;

struct Recall {
    double sum;
    double cnt;
};

int main (int argc, char *argv[]) {

    int K;

    namespace po = boost::program_options; 
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message.")
        (",K", po::value(&K)->default_value(20), "")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm); 

    if (vm.count("help"))
    {
        cout << desc;
        return 0;
    }

    vector<char> tmp(KNN::size(K));

    while (cin.read(&tmp[0], tmp.size())) {
        KNN *knn = (KNN *)&tmp[0];
        cout << knn->id;
        for (int i = 0; i < K; ++i) {
            cout << ' ' << '(' << knn->data[i].nn << ',' << knn->data[i].dist << ',' << knn->data[i].isnew << ')';
        }
        cout << endl;
    }

    return 0;
}

