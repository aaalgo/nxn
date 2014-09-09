/*
static const char *COPYRIGHT =
    "Cluster K-NN graph constructor.\n"
    "Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All Rights Reserved.\n"
    "Do not redistribute the program in either binary or source code form.\n"
    "\n";
    */
#include <xmmintrin.h>
#include <cmath>
#include <cstring>
#include <limits>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>
#include <boost/assert.hpp>
#include <boost/timer.hpp>
#include <boost/foreach.hpp>
#include <boost/random.hpp>
#include <boost/program_options.hpp>
#include <nxn.h>
#include <nxn++.h>
#include <lshkit/matrix.h>
#include <lshkit/matrix-io.h>

using namespace std;
using namespace nxn;

// One feature vector, with dynamic size
// Must call Feature::setDim to initialize the dimension before
// Any instance is to be used.
struct Feature {
    static int dim;
    int32_t id;
    float desc[1];
    void copy (const Feature &ff) {
        memcpy((char *)this, (const char *)&ff, size());
    }
    static void setDim (int D) {
        dim = D;
    }
    static size_t size () {
        BOOST_VERIFY(dim >= 1);
        return sizeof(Feature) + sizeof(float) * (dim-1);
    }
    static float dist (Feature const &f1, Feature const &f2)  {
        float d = 0;
        for (int i = 0; i < dim; i++) {
            float v = f1.desc[i] - f2.desc[i];
            d += v * v;
        }
        return sqrt(d);
    }
} __attribute__((__packed__));

int Feature::dim = -1;

// One candidate, static size
struct Candidate {  // Candidate
    int32_t id;
    int32_t nn;
    float dist;     // distance between id and nn
    friend bool operator < (const Candidate &a, const Candidate &b) {
        if (a.id == b.id) {
            if (a.dist == b.dist) return a.nn < b.nn;
            return a.dist < b.dist;
        }
        return a.id < b.id;
    }
};

// K-NN, dynamic typed
struct KNN {
    int32_t sn;
    int32_t id;
    struct Entry {
        int32_t nn;
//        float radius;
        float dist;
    } __attribute__((__packed__)) data[1];
    static size_t size (int K) {
        return sizeof(KNN) + sizeof(KNN::Entry) * (K-1);
    }
    void init (int sn_, int id_, int K) {
        sn = sn_;
        id = id_;
        for (int i = 0; i < K; ++i) {
            data[i].nn = -1;
            data[i].dist = numeric_limits<float>::max();
        }
    }
    void update (int K, float dist, int nn) {
        // update K-NN
        int i = -1; // insert position

        if (nn == id) return;
        if (dist < data[K-1].dist) {
            i = K - 1;
            while (i > 0) {
                int j = i - 1;
                if (data[j].nn == nn) {
                    i = -1;
                    break;
                }
                if (data[j].dist < dist) break;
                i = j;
            }
        }

        if (i >= 0) { // to be inserted
            BOOST_VERIFY(i < K);
            for (int j = K-1; j > i; --j) {
                data[j] = data[j-1];
            }
            data[i].nn = nn;
            data[i].dist = dist;
        }
    }
} __attribute__((__packed__));

    size_t zipper = 0;
    size_t mem_size = 0;     // mem in GB
    size_t block_size = 0;   // block in MB
    string dir;         // intermediate directory
    string output_path;
    string bench_path;
    int part = -1;

    int D = -1;         // dimensionality
    int Q = -1;

    // import parameters

    int K = -1;
    int per_part = -1;


int sample_routine (void *u_ptr, int part, nxn_segment_t segs[]) {
    dynamic_vector_view<Feature> all(segs[0], Feature::size());
    vector<int> ids(per_part);
    for (int i = 0; i < per_part; i++) {
        for (;;) {
            int id = rand () % all.size();
            for (int j = 0; j < i; ++j) {
                if (id == ids[j]) {
                    id = -1;
                    break;
                }
            }
            if (id >= 0) {
                ids[i] = id;
                break;
            }
        }
    }
    BOOST_FOREACH(int id, ids) {
        nxn_write(0, 0, &all[id]);
    }
    return 0;
}

int gather_routine (void *u_ptr, int part, nxn_segment_t segs[]) {
    dynamic_vector_view<Feature> all(segs[0], Feature::size());
    if (all.size() > 0) {
        BOOST_VERIFY((int)all.size() >= Q);
        ofstream os(bench_path.c_str(), ios::binary);
        for (int i = 0; i < Q; ++i) {
            os.write((const char *)&all[i], Feature::size());
        }
        os.close();
    }
    return 0;
}


vector<char> bench_data;
nxn_segment_t bench_buf;
dynamic_vector_view<Feature> bench;
vector<char> KNN_data;
vector<KNN *> knns;

void init_knns () {
    KNN_data.resize(Q * KNN::size(K));
    knns.resize(Q);

    for (int i = 0; i < Q; ++i) {
        knns[i] = (KNN *)&KNN_data[i * KNN::size(K)];
        knns[i]->init(i, bench[i].id, K);
    }
}

int knn_prelude (void *u_ptr) {
    ifstream is(bench_path.c_str(), ios::binary);
    BOOST_VERIFY(is);
    is.seekg(0, ios::end);
    size_t len = is.tellg();
    bench_data.resize(len);
    is.seekg(0);
    is.read(&bench_data[0], len);
    BOOST_VERIFY(is);
    is.close();

    bench_buf.data = &bench_data[0];
    bench_buf.len = bench_data.size();

    bench.reset(bench_buf, Feature::size());

    init_knns();
    return 0;
}

int knn_routine (void *u_ptr, int part, nxn_segment_t segs[]) {
    dynamic_vector_view<Feature> all(segs[0], Feature::size());
    for (unsigned cc = 0; cc < all.size(); ++cc) {
        for (int q = 0; q < Q; ++q) {
            knns[q]->update(K, Feature::dist(all[cc], bench[q]), all[cc].id);
        }
    }
    return 0;
}

int knn_postlude (void *u_ptr) {
    for (int i = 0; i < Q; ++i) {
        nxn_write(0, 0, knns[i]);
    }
    return 0;
}

int merge_prelude (void *u_ptr) {
    init_knns();
    return 0;
}

int merge_routine (void *u_ptr, int part, nxn_segment_t segs[]) {

    dynamic_vector_view<KNN> all(segs[0], KNN::size(K));
    for (unsigned cc = 0; cc < all.size(); ++cc) {
        KNN const *cand = &all[cc];
        KNN *knn = knns[cand->sn];
        BOOST_VERIFY(knn->sn == cand->sn);
        if (knn->id < 0) knn->id = cand->id;
        else BOOST_VERIFY(knn->id == cand->id);
        for (int k = 0; k < K; ++k) {
            knn->update(K, cand->data[k].dist, cand->data[k].nn);
        }
    }
    ofstream os(output_path.c_str(), ios::binary | ios::trunc);
    struct {
        int32_t size;
        int32_t N;
        int32_t dim;
    } header;
    header.size = sizeof(int32_t);
    header.N = Q;
    header.dim = K + 1;
    os.write((const char *)&header, 3 * sizeof(int32_t));
    for (int q = 0; q < Q; ++q) {
        os.write((const char *)&knns[q]->id, sizeof(int32_t));
        for (int k = 0; k < K; ++k) {
            os.write((const char *)&knns[q]->data[k].nn, sizeof(int32_t));
        }
    }
    os.close();
    return 0;
}

int main (int argc, char *argv[]) {


    namespace po = boost::program_options; 
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message.")
        ("mem", po::value(&mem_size)->default_value(2), "mem in GB")
        ("block", po::value(&block_size)->default_value(2), "block in MB")
        ("zipper", po::value(&zipper)->default_value(0), "")
        ("dir,d", po::value(&dir)->default_value("knng"), "")
        (",P", po::value(&part)->default_value(500), "")
        (",D", po::value(&D)->default_value(128), "")
        (",K", po::value(&K)->default_value(100), "")
        (",Q", po::value(&Q)->default_value(1000), "")
        ("output,O", po::value(&output_path), "shared output")
        ("bench,B", po::value(&bench_path), "shared benchmark")
        ("help,h", "")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm); 

    if (vm.count("help") || (vm.count("output") == 0)) {
        cout << desc;
        return 0;
    }

    if (bench_path.empty()) bench_path = output_path;

    Feature::setDim(D);

    nxn_startup();

    nxn_job_t job;
    job.zipper = zipper;
    job.dir = dir.c_str();
    job.total_size = 1024L * 1024 * 1024 * mem_size;
    job.block_size = 1024 * 1024 * block_size;
    job.npart = part;
    job.u_ptr = 0;
    job.worker = 1;

    // sample points
    //
    per_part = (Q + part - 1) / part;

    srand(nxn_rank());

    nxn_report_0("SAMPLE");
    {
        nxn_local_t localv[] = {{"orig", 0}, {NULL, 0}};
        nxn_remote_t remotev[] = 
                    { {"bench", 0, Feature::size()},
                      {NULL, 0, 0}
                    };
        job.localv = localv;
        job.remotev = remotev;
        job.prelude = NULL;
        job.postlude = NULL;
        job.routine = sample_routine;
        nxn_run(&job);
    }
    nxn_report_0("GATHER");
    {
        nxn_local_t localv[] = {{"bench", 0}, {NULL, 0}};
        nxn_remote_t remotev[] = {{NULL, 0, 0}};
        job.localv = localv;
        job.remotev = remotev;
        job.prelude = NULL;
        job.postlude = NULL;
        job.routine = gather_routine;
        nxn_run(&job);

    }

    nxn_report_0("KNN");
    {
        nxn_local_t localv[] = {{"orig", 0}, {NULL, 0}};
        nxn_remote_t remotev[] = {
                    {"gs", 0, KNN::size(K)},
                    {NULL, 0, 0}};
        job.localv = localv;
        job.remotev = remotev;
        job.prelude = knn_prelude;
        job.postlude = knn_postlude;
        job.routine = knn_routine;
        nxn_run(&job);
    }

    nxn_report_0("MERGE");
    {
        nxn_local_t localv[] = {{"gs", 0}, {NULL,0}};
        nxn_remote_t remotev[] = {
                    {NULL, 0, 0}
                    };
        job.localv = localv;
        job.remotev = remotev;
        job.prelude = merge_prelude;
        job.postlude = NULL;
        job.routine = merge_routine;
        nxn_run(&job);
    }

    nxn_shutdown();

    return 0;
}

