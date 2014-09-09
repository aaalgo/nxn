static const char *COPYRIGHT =
    "Cluster K-NN graph constructor.\n"
    "Copyright (C) 2011 Wei Dong <wdong.pku@gmail.com>. All Rights Reserved.\n"
    "Do not redistribute the program in either binary or source code form.\n"
    "\n";
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

class Features: public nxn::dynamic_vector_view<Feature> {
    std::map<int, int> map;
    std::vector<int> count;
    size_t bytes;
public:
    const std::map<int, int> &getmap () const {
        return map;
    }

    Features (const nxn_segment_t &buf): nxn::dynamic_vector_view<Feature>(buf, Feature::size()), bytes(buf.len) {
        count.resize(size());
        std::fill(count.begin(), count.end(), 0);
        for (unsigned i = 0; i < size(); ++i) {
            map[at(i).id] = i;
        }
    }

    Feature const *get (int id) const {
        std::map<int, int>::const_iterator v = map.find(id);
        if (v == map.end()) return NULL;
        return &at(v->second);
    }

    Feature const *getref (int id) {
        std::map<int, int>::const_iterator v = map.find(id);
        if (v == map.end()) return NULL;
        ++count[v->second];
        return &at(v->second);
    }

    // remove features not refered,
    // after this is called, the 
    bool pack (float factor) {
        size_t cnt = 0;
        for (size_t i = 0; i < size(); ++i) {
            if (count[i]) ++cnt;
        }
        if (cnt >= (size() * (1.0 - factor))) return false;
        size_t last = 0;
        for (size_t i = 0; i < size(); ++i) {
            if (count[i]) {
                at(last).copy(at(i));
                ++last;
            }
        }
        shrink(last);
        map.clear();
        count.clear();
        return true;
    }
};


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

// One reverse K-NN, static size
struct RNN {        // K-NN update, also used as R-NN list
                    // this is sent to "nn", and "nn" should send its feature to "id"
                    // "nn" should add this to its R-NN list
                    // and also send back its own feature.
    int32_t id;
    int32_t nn;
//    float radius;   // < 0: to be removed
    enum {                  // possible values
                            // 0
        INIT = 0x01,        // use in local join, discard afterwards
        REVERSE = 0x02,     // simply reverse, discard afterwards,
        REMOVE = 0x04,      // remove existing RNN (0) of the same <id, nn>
        ADD = 0x08,         // use in local join, reverse, change to 0 afterwards
        OLD = 0x10,         // only joined with new one, keep the same
    } type;
    friend bool remove_seq (const RNN &a, const RNN &b) {
        return (a.id == b.id) && (a.nn == b.nn) ; // && (a.type == RNN::REMOVE) && (b.type == RNN::OLD);
    }
    friend bool operator < (const RNN &a, const RNN &b) {
        if (a.nn == b.nn) {
            if (a.id == b.id) {
                return a.type < b.type;
            }
            return a.id < b.id;
        }
        return a.nn < b.nn;
    }
} __attribute__((__packed__));

// K-NN, dynamic typed
struct KNN {
    enum {
        OLD = 0,
        NEW = 1,
        PICKED = 2
    };
    int32_t id;
    struct Entry {
        int32_t nn;
//        float radius;
        float dist;
        int32_t type;
    } __attribute__((__packed__)) data[1];
    static size_t size (int K) {
        return sizeof(KNN) + sizeof(KNN::Entry) * (K-1);
    }
} __attribute__((__packed__));


struct Recall {
    double sum;
    double cnt;
};

// global parameters

int zipper = 0;
int worker = 0;
size_t mem_size = 0;     // mem in GB
size_t block_size = 0;   // block in MB
string dir;         // intermediate directory
int part = -1;

int D = -1;         // dimensionality

// import parameters
string import_path;
int import_stream = 0;
int import_random = 0;
int skip = 0;
int pad = 0;
bool global = false;
int total = -1;     // # points

// benchmark
string gs_path;
lshkit::Matrix<int> gs;
map<int, int *> gsmap;

// algorihtm parameters
bool init = true;
int K = -1;
int sample = -1;
int maxit = -1;
double T = 0;

// optimization parameters
int initK = 0;
bool shuffle = false;
float pack_factor = 0;

int rank = -1;
int size = -1;

pthread_mutex_t lock;

int import_prelude (void *u_ptr) {
    ifstream is;

    int N = 0;
    if (import_path.size()) {
        if ((!global) || (rank < import_stream)) {
            is.open(import_path.c_str(), ios::binary);
            if (is) {
                is.seekg(0, ios::end);
                size_t len = is.tellg();
                N = (len  - skip) / ((D - pad) * sizeof(float));
                BOOST_VERIFY((len - skip) % N == 0);
                if (!global) {
                    is.seekg(skip);
                    BOOST_VERIFY(is);
                }
                else {  // local
                    int perS = (N + import_stream - 1) / import_stream; // each stream
                    int start = rank * perS;
                    is.seekg(skip + ((D - pad) * sizeof(float) * start));
                    BOOST_VERIFY(is);
                    if (start + perS > N) N = N - start;
                    else N = perS;
                }
            }
        }
    }
    else {
        if ((!global) || (rank < import_stream)) {
            N = import_random;
        }
    }

    vector<int> all(size);

    nxn_scatter(&N, sizeof(N), &all[0]);

    int start = 0;
    for (int i = 0; i < rank; ++i) {
        start += all[i];
    }

    total = start;
    for (int i = rank; i < size; i++) {
        total += all[i];
    }
    
    nxn_report_0("TOTAL: %d", total);

    vector<char> tmp(Feature::size());
    Feature *record = (Feature *)&tmp[0];

    boost::mt19937 rng;
    boost::variate_generator<boost::mt19937 &, boost::uniform_real<float> > uniform(rng, boost::uniform_real<float>(0,1.0));

    fill(record->desc, record->desc + D, 0.0F);

    for (int i = 0; i < N; i++) {
        record->id = start + i;
        if (import_random) {
            for (int j = 0; j < D - pad; ++j) {
                record->desc[j] = uniform();
            }
        }
        else {
            is.read((char *)record->desc, (D - pad) * sizeof(float));
            BOOST_VERIFY(is);
        }
        nxn_write(0, record->id, record);
    }
    return 0;
}

int init_prelude (void *u_ptr) {
    if (total < 0) {
        long long N = nxn_local_size(0) / Feature::size();
        N = nxn_sum(N);
        BOOST_VERIFY(N <= numeric_limits<int>::max());
        total = (int)N;
        nxn_report_0("\tTOTAL: %d", total);
    }
    return 0;
}

int init_routine (void *u_ptr, int part, nxn_segment_t segs[]) {
    dynamic_vector_view<Feature> all(segs[0], Feature::size());
    for (unsigned i = 0; i < all.size(); ++i) {
        const Feature &f = all[i];
        for (int j = 0; j < initK; j++) {
            RNN nn;
            nn.id = f.id;
            nn.nn = rand() % total;
            nn.type = RNN::REVERSE;
            nxn_write(0, nn.nn, &nn);
            nn.id = nn.nn;
            nn.nn = f.id;
            nn.type = RNN::INIT;
            nxn_write(0, nn.nn, &nn);
        }
    }
    return 0;
}

int reverse_routine (void *u_ptr, int part, nxn_segment_t segs[]) {
    Features orig(segs[0]);
    vector_view<RNN> rnn(segs[1]);
    BOOST_FOREACH(const RNN &up, rnn) {
        if ((up.type == RNN::REVERSE) || (up.type == RNN::ADD)) {
            const Feature *ff = orig.get(up.nn);
            BOOST_VERIFY(ff);
            nxn_write(0, up.id, ff);
        }
    }
    return 0;
}

long long orig_feature_size = 0;
long long new_feature_size = 0;
long long cost = 0;
long long total_cost = 0;

int local_join_prelude (void *u_ptr) {
    /*
    long long max_input = nxn_max_input_size();
    max_input = nxn_max(max_input);
    nxn_report_0("\t\tMAX INPUT: %lld", max_input);
    */
    cost = 0;
    orig_feature_size = 0;
    new_feature_size = 0;
    return 0;
}

int local_join_postlude (void *u_ptr) {
    cost = nxn_sum(cost);
    total_cost += cost;
    orig_feature_size = nxn_sum(orig_feature_size);
    new_feature_size = nxn_sum(new_feature_size);

    nxn_report_0("\t\t%lld pairs compared.", cost);
    nxn_report_0("\t\tfeatures: %lld -> %lld", orig_feature_size, new_feature_size);
    return 0;
}

int local_join_routine (void *u_ptr, int cur, nxn_segment_t segs[]) {

    long long mycost = 0;

    dynamic_vector_view<KNN> knns(segs[0], KNN::size(K));
    vector_view<RNN> rnns(segs[1]);
    Features features(segs[2]);

    sort(rnns.begin(), rnns.end());

    int id = 0;
    unsigned int rnn_last = 0;
    unsigned int rnn_next = 0;
    unsigned int knn_next = 0;
    // for each id
    while ((knn_next < knns.size()) || (rnn_next < rnns.size())) {
        // determine the next id to work with
        bool use_knn = false;
        if (knn_next < knns.size()) {
            use_knn = true;
            id = knns[knn_next].id;
        }
        if (rnn_next < rnns.size()) {
            if ((!use_knn) || (rnns[rnn_next].nn < id)) {
                use_knn = false;
                id = rnns[rnn_next].nn;
            }
        }

        map<int, const Feature *> oldones;
        map<int, const Feature *> newones;

        // if there is a KNN for that id
        if (use_knn) {
            BOOST_FOREACH(KNN::Entry const &e, 
                    make_pair(knns[knn_next].data, knns[knn_next].data + K)) {
                if ((e.nn < 0) || (e.type == KNN::NEW)) continue;
                Feature const *ff = features.getref(e.nn);
                BOOST_VERIFY(ff);
                if (e.type == KNN::PICKED) { // add to new ones
                    newones[e.nn] = ff;
                }
                else if (e.type == KNN::OLD) {  // add to old ones
                    oldones[e.nn] = ff;
                }
            }
            ++knn_next;
        }

        // update rnn list
        while ((rnn_next < rnns.size()) && (rnns[rnn_next].nn == id)) {

            RNN &rnn = rnns[rnn_next];
            switch(rnn.type) {
                case RNN::INIT: newones[rnn.id] = features.get(rnn.id);
                                ++rnn_next; // then discard
                                break;
                case RNN::REVERSE:  // silently discard
                                ++rnn_next;
                                break;
                case RNN::REMOVE:
                                // !!! double check this, possible bug
                                if ((rnn_next + 1 < rnns.size()) && remove_seq(rnns[rnn_next], rnns[rnn_next+1])) {
                                    rnn_next += 2;
                                }
                                else {
                                    rnn_next += 1;
                                }
                                break;
                case RNN::ADD: // add to new ones
                                if (oldones.find(rnn.id) == oldones.end()) {
                                    Feature const *ff = features.getref(rnn.id);
                                    BOOST_VERIFY(ff);
                                    newones[rnn.id] = ff;
                                }
                                rnns[rnn_next].type = RNN::OLD;
                                rnns[rnn_last] = rnns[rnn_next];
                                ++rnn_last;
                                ++rnn_next;
                                break;
                case RNN::OLD: // add to old ones
                                {
                                    Feature const *ff = features.getref(rnn.id);
                                    BOOST_VERIFY(ff);
                                    oldones[rnn.id] = ff;
                                }
                                rnns[rnn_last] = rnns[rnn_next];
                                ++rnn_last;
                                ++rnn_next;
                                break;
                default:        BOOST_VERIFY(0);
            }
            ++rnn_next;
        }

        pair<int, const Feature *> nv, nu;
        BOOST_FOREACH(nv, newones) {
            BOOST_FOREACH(nu, newones) {
                if (nu.first >= nv.first) break;

                Candidate cand;
                cand.id = nv.first;
                cand.nn = nu.first;
                BOOST_VERIFY(nu.second);
                BOOST_VERIFY(nv.second);
                cand.dist = Feature::dist(*nu.second, *nv.second);

                nxn_write(0, cand.id, &cand);
                swap(cand.id, cand.nn);
                nxn_write(0, cand.id, &cand);
                ++mycost;
            }
            BOOST_FOREACH(nu, oldones) {
                if (nu.first == nv.first) continue;

                Candidate cand;
                cand.id = nv.first;
                cand.nn = nu.first;
                BOOST_VERIFY(nu.second);
                BOOST_VERIFY(nv.second);
                cand.dist = Feature::dist(*nu.second, *nv.second);
                nxn_write(0, cand.id, &cand);
                nxn_write(0, cand.id, &cand);
                swap(cand.id, cand.nn);
                nxn_write(0, cand.id, &cand);
                ++mycost;
            }
        }
    }
    // write back updated rnn
    segs[1].len = sizeof(RNN) * rnn_last;
    nxn_write_local(cur, 1, &segs[1]);

    long long orig = segs[2].len / Feature::size();

    if (features.pack(pack_factor)) {
        segs[2].len = features.size() * Feature::size();
        nxn_write_local(cur, 2, &segs[2]);
    }

    int ret = pthread_mutex_lock(&lock);
    BOOST_VERIFY(ret == 0);
    cost += mycost;
    orig_feature_size += orig;
    new_feature_size += segs[2].len / Feature::size();
    ret = pthread_mutex_unlock(&lock);
    BOOST_VERIFY(ret == 0);
    return 0;
}

Recall recall;
long long c_update = 0;
double r_update = 0;

int update_prelude (void *u_ptr) {
    recall.sum = recall.cnt = 0;
    c_update = 0;
    return 0;
}

int update_postlude (void *u_ptr) {
    c_update = nxn_sum(c_update);

    vector<Recall> recalls(nxn_size());
    nxn_scatter(&recall, sizeof(Recall), &recalls[0]);

    recall.sum = 0;
    recall.cnt = 0;
    BOOST_FOREACH(Recall r, recalls) {
        recall.sum += r.sum;
        recall.cnt += r.cnt;
    }

    double avg = 0;
    if (recall.cnt > 0) avg = recall.sum / recall.cnt;

    nxn_report_0("\tRECALL: %f", avg);

    double brute_force_cost = double(total) * (total -1)/2;
    nxn_report_0("\tCOST: %f (%f)", total_cost / brute_force_cost,
                                          cost / brute_force_cost);
    r_update = double(c_update) / (K * total);
    nxn_report_0("\tUPDATE: %f", r_update);
    return 0;
}

int update_routine (void *u_ptr, int cur, nxn_segment_t segs[]) {

    Recall myrecall;
    long long myupdate = 0;

    myrecall.sum = myrecall.cnt = 0;

    Features orig(segs[0]);
    BOOST_VERIFY(orig.size());
    dynamic_vector_view<KNN> knns(segs[1], KNN::size(K));
    vector_view<Candidate> candy(segs[2]);
    if (knns.size() == 0 && candy.size() == 0) {
        nxn_report("node %d, funny: )", rank);
    }

    vector<KNN *> pknns(nxn_max_part_size(total, part));
    vector<vector<char> > tmp(pknns.size());

    fill(pknns.begin(), pknns.end(), (KNN *)0);


    for (unsigned i = 0; i < knns.size(); ++i) {
        int rr = nxn_in_part_rank(knns[i].id, part);
        pknns[rr] = &knns[i];
        for (int k = 0; k < K; ++k) {
            if (pknns[rr]->data[k].type == KNN::PICKED) {
                pknns[rr]->data[k].type = KNN::OLD;
            }
        }
    }


    int fd = nxn_local_fd(cur, 1);
    BOOST_VERIFY(fd >= 0);

    BOOST_FOREACH(Candidate cc, candy) {
        int rr = nxn_in_part_rank(cc.id, part);
        if (pknns[rr] == 0) {
            tmp[rr].resize(KNN::size(K));
            pknns[rr] = (KNN *)&(tmp[rr][0]);
            pknns[rr]->id = cc.id;
            for (int k = 0; k < K; ++k) {
                pknns[rr]->data[k].nn = -1;
                pknns[rr]->data[k].dist = numeric_limits<float>::max();
                pknns[rr]->data[k].type = KNN::OLD;
            }
        }

        if (pknns[rr]->id != cc.id) {
            nxn_report("node %d @ %d of %d, rr.id %d cc.id %d", rank, rr, pknns.size(), pknns[rr]->id, cc.id);
        }

        BOOST_VERIFY(pknns[rr]->id == cc.id);
        KNN::Entry *data = pknns[rr]->data;
        // update K-NN
        {
            int i = -1; // insert position

            if (cc.dist < data[K-1].dist) {
                i = K - 1;
                while (i > 0) {
                    int j = i - 1;
                    if (data[j].nn == cc.nn) {
                        i = -1;
                        break;
                    }
                    if (data[j].dist < cc.dist) break;
                    i = j;
                }
            }

            if (i >= 0) { // to be inserted
                BOOST_VERIFY(i < K);

                if ((data[K-1].nn >= 0) && (data[K-1].nn != KNN::NEW)) {
                    RNN rnn;
                    rnn.id = cc.id;
                    rnn.nn = data[K-1].nn;
                    rnn.type = RNN::REMOVE;
                    nxn_write(0, rnn.nn, &rnn);
                }

                for (int j = K-1; j > i; --j) {
                    data[j] = data[j-1];
                }
                data[i].nn = cc.nn;
                data[i].dist = cc.dist;
                data[i].type = KNN::NEW;
            }
        }
    }


    for (unsigned i = 0; i < pknns.size(); ++i) {
        KNN *knn = pknns[i];
        if (knn == 0) continue;
        map<int, int*>::const_iterator it = gsmap.find(knn->id);
        if (it != gsmap.end()) {
            KNN::Entry *ans = knn->data;
            int *true_nn = it->second;
            int found = 0;
            for (int i = 0; i < K; i++) {
                for (int j = 0; j < K; j++) {
                    if (true_nn[i] == ans[j].nn) {
                        found++;
                        break;
                    }
                }
            }
            myrecall.sum += double(found) / K;
            myrecall.cnt += 1.0;
        }
        const Feature *ff = orig.get(knn->id);
        BOOST_VERIFY(ff);

        vector<int> picked;

        for (int i = 0; i < K; i++) {
            if (!(knn->data[i].type == KNN::NEW)) continue;
            picked.push_back(i);
            ++myupdate;
        }

        if ((int)picked.size() > sample) {
            if (shuffle) {
                random_shuffle(picked.begin(), picked.end());
            }
            picked.resize(sample);
        }

        BOOST_FOREACH(int i, picked) {
            knn->data[i].type = KNN::PICKED;
            RNN rnn;
            rnn.id = knn->id;
            rnn.nn = knn->data[i].nn;

            rnn.type = RNN::ADD;
            nxn_write(0, rnn.nn, &rnn);
            nxn_write(1, rnn.nn, ff);
        }

        int ret = write(fd, knn, KNN::size(K));
        BOOST_VERIFY(ret == (int)KNN::size(K));
    }

    // update stats
    int ret = pthread_mutex_lock(&lock);
    BOOST_VERIFY(ret == 0);
    recall.sum += myrecall.sum;
    recall.cnt += myrecall.cnt;
    c_update += myupdate;
    ret = pthread_mutex_unlock(&lock);
    BOOST_VERIFY(ret == 0);

    ret = close(fd);
    BOOST_VERIFY(ret == 0);
    return 0;
}

int main (int argc, char *argv[]) {

    float initfactor = 0;
    float S = -1;

    namespace po = boost::program_options; 
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message.")
        ("mem", po::value(&mem_size)->default_value(2), "mem in GB")
        ("block", po::value(&block_size)->default_value(2), "block in MB")
        ("zipper", po::value(&zipper)->default_value(0), "")
        ("worker", po::value(&worker)->default_value(1), "")
        ("dir,d", po::value(&dir)->default_value("knng"), "")
        (",P", po::value(&part)->default_value(500), "")
        (",D", po::value(&D)->default_value(128), "")
        ("import", po::value(&import_path), "")
        ("stream", po::value(&import_stream)->default_value(1), "")
        ("skip", po::value(&skip)->default_value(0), "skip bytes in input")
        ("pad", po::value(&pad)->default_value(0), "")
        ("gen", po::value(&import_random)->default_value(0), "generate random data")
        ("global", "only import/generate data at node 0")
        ("gs", po::value(&gs_path), "gold standard")
        (",K", po::value(&K)->default_value(20), "")
        (",S", po::value(&S)->default_value(1.0), "sample, 0 - 1")
        (",I", po::value(&maxit)->default_value(20), "")
        (",T", po::value(&T)->default_value(0.001), "")
        ("initfactor", po::value(&initfactor)->default_value(2.0), "")
        ("continue,c", "do not import/initialize, continue with existing data")
        ("shuffle", "random sample")
        ("pack", po::value(&pack_factor)->default_value(0.0), "")
        ("help,h", "")
        ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm); 

    if (vm.count("help") 
            || (vm.count("import") && (import_random > 0))
            || ((vm.count("import") || (import_random > 0)) && vm.count("continue") && maxit)
            || (pad >= D)) {
        cout << desc;
        cout << "Use --import to import data, or use --gen to generate random data, but you can choose only one." << endl;
        cout << "Use --continue to skip initialization and continue with previous partial results.  Cannot be combined with --import or --gen." << endl;
        return 0;
    }

    if (vm.count("continue")) init = false;
    if (vm.count("global")) global = true;
    if (vm.count("shuffle")) shuffle = true;

    Feature::setDim(D);

    sample = K * S;
    initK = K * initfactor;

    if (gs_path.size()) {
        gs.load(gs_path);
        for (int i = 0; i < gs.getSize(); ++i) {
            gsmap[gs[i][0]] = gs[i] + 1;
        }
    }

    nxn_startup();

    int ret = pthread_mutex_init(&lock, NULL);
    BOOST_VERIFY(ret == 0);

    nxn_report_0(COPYRIGHT);

    rank = nxn_rank();
    size = nxn_size();
    cost = 0;
    total_cost = 0;

    BOOST_VERIFY(import_stream >= 0);
    BOOST_VERIFY(import_stream <= size);

    nxn_job_t job;

    job.dir = dir.c_str();
    job.u_ptr = NULL;
    job.npart = part;
    job.zipper = zipper;
    job.worker = worker;
    job.total_size = 1024L * 1024 * 1024 * mem_size;
    job.block_size = 1024 * 1024 * block_size;

    long long this_program = nxn_time();

    if (import_path.size() || (import_random > 0)) {
        long long this_job = nxn_time();
        nxn_report_0("IMPORT DATA");
        nxn_local_t localv[] = {{NULL,0}};
        nxn_remote_t remotev[] = {
                    {"orig", 0, Feature::size()},
                    {NULL, 0, 0}};
        job.localv = localv;
        job.remotev = remotev;
        job.prelude = import_prelude;
        job.postlude = NULL;
        job.routine = NULL;
        nxn_run(&job);
        nxn_report_0("TIME: %gs", (nxn_time() - this_job) / 1000.0);
    }

    if (init) {
        long long this_job = nxn_time();
        nxn_report_0("INIT");
        nxn_local_t localv[] = {{"orig", 0}, {NULL, 0}};
        nxn_remote_t remotev[] = 
                    { {"rnn", 0, sizeof(RNN)},
                      {"knn", 0, KNN::size(K)},
                      {"feat", 0, Feature::size()},
                      {"candy", 0, sizeof(Candidate)},
                      {NULL, 0, 0}
                    };
        job.localv = localv;
        job.remotev = remotev;
        job.prelude = init_prelude;
        job.postlude = NULL;
        job.routine = init_routine;
        srand(nxn_rank());
        nxn_run(&job);
        nxn_report_0("TIME: %gs", (nxn_time() - this_job) / 1000.0);
    }

    for (int it = 0; it < maxit; ++it) {
        long long this_iteration = nxn_time();
        nxn_report_0("ITERATION %d", it);
        {
            long long this_job = nxn_time();
            nxn_report_0("\tREVERSE");
            nxn_local_t localv[] = {
                            {"orig", 0},
                            {"rnn", 0},      // merge update & write back   
                            {NULL, 0}};    
            nxn_remote_t remotev[] =  {
                            {"feat", 1, Feature::size()},
                            {NULL, 0, 0}};
            job.localv = localv;
            job.remotev = remotev;
            job.prelude = init_prelude;
            job.postlude = NULL;
            job.routine = reverse_routine;
            nxn_run(&job);
            nxn_report_0("\tTIME: %gs", (nxn_time() - this_job) / 1000.0);
        }
        {  
            long long this_job = nxn_time();
            nxn_report_0("\tLOCAL JOIN");
            nxn_local_t localv[] = {
                            {"knn", 0},  // ADD, OLD
                            {"rnn", 0}, // update
                            {"feat", 0}, // possibly update (compressed)
                            {NULL, 0}};  
            nxn_remote_t remotev[] = {
                            {"candy", 0, sizeof(Candidate)},
                            {NULL, 0, 0}};
            job.localv = localv;
            job.remotev = remotev;
            job.prelude = local_join_prelude;
            job.postlude = local_join_postlude;
            job.routine = local_join_routine;
            nxn_run(&job);
            nxn_report_0("\tTIME: %gs", (nxn_time() - this_job) / 1000.0);
        }
        {  
            long long this_job = nxn_time();
            nxn_report_0("\tUPDATE KNN");
            nxn_local_t localv[] = {
                            {"orig", 0},
                            {"knn", 0},
                            {"candy", 0},
                            {NULL, 0}};
            nxn_remote_t remotev[] = {
                            {"rnn", 1, sizeof(RNN)},
                            {"feat", 1, Feature::size()}, // append
                            {NULL, 0, 0}};

            job.localv = localv;
            job.remotev = remotev;
            job.prelude = update_prelude;
            job.postlude = update_postlude;
            job.routine = update_routine;
            nxn_run(&job);
            nxn_report_0("\tTIME: %gs", (nxn_time() - this_job) / 1000.0);
        }
        nxn_report_0("TIME: %gs", (nxn_time() - this_iteration) / 1000.0);
        nxn_report_0("ELAPSED: %gs", (nxn_time() - this_program) / 1000.0);
        if (!(r_update > T)) break;
    }

    ret = pthread_mutex_destroy(&lock);
    BOOST_VERIFY(ret == 0);


    nxn_shutdown();

    return 0;
}

