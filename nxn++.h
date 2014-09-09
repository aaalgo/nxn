#ifndef WDONG_NXNPLUSPLUS
#define WDONG_NXNPLUSPLUS

#include <vector>
#include <map>

namespace nxn {

    long long nxn_sum (long long v) {
        std::vector<long long> all(nxn_size());
        nxn_scatter(&v, sizeof(long long), &all[0]);
        long long r = 0;
        for (unsigned i = 0; i < all.size(); ++i) {
            r += all[i];
        }
        return r;
    }

    long long nxn_max (long long v) {
        std::vector<long long> all(nxn_size());
        nxn_scatter(&v, sizeof(long long), &all[0]);
        long long r = all[0];
        for (unsigned i = 1; i < all.size(); ++i) {
            if (all[i] > r) r = all[i];
        }
        return r;
    }

    template <typename T>
    class vector_view {
        T *data;
        size_t s;
    public:
        typedef T* iterator;
        typedef T const * const_iterator;
        vector_view (const nxn_segment_t &buf): data((T *)buf.data), s(buf.len / sizeof(T)) {
            BOOST_VERIFY(buf.len % sizeof(T) == 0);
        }

        vector_view () {
            data = 0;
            s = 0;
        }

        void reset (const nxn_segment_t &buf) {
            data = (T *)buf.data;
            s = buf.len / sizeof(T); 
            BOOST_VERIFY(buf.len % sizeof(T) == 0);
        }

        size_t size () const {
            return s;
        }

        void shrink (size_t news) {
            BOOST_VERIFY(news <= s);
            s = news;
        }


        T *begin () {
            return data;
        }

        T *end () {
            return data + s;
        }

        T const *const_begin () const {
            return data;
        }

        T const *const_end () const {
            return data + s;
        }

        T &at (int i) {
            return data[i];
        }

        T const &at (int i) const {
            return data[i];
        }

        T &operator [] (int i) {
            return at(i);
        }

        T const &operator [] (int i) const {
            return at(i);
        }
    };

    template <typename T>
    class dynamic_vector_view {
        char *data;
        size_t s;
        size_t step;
    public:
        dynamic_vector_view () {
            data = NULL;
            s = step = 0;
        }
        dynamic_vector_view (const nxn_segment_t &buf, size_t st): data((char *)buf.data), s(buf.len / st), step(st) {
            BOOST_VERIFY((buf.len % st) == 0);
        }

        void reset (const nxn_segment_t &buf, size_t st) {
            data = (char *)buf.data;
            s = buf.len / st;
            step = st;
            BOOST_VERIFY((buf.len % st) == 0);
        }

        void shrink (size_t news) {
            BOOST_VERIFY(news <= s);
            s = news;
        }

        size_t size () const {
            return s;
        }

        T &at (int i) {
            return *(T *)(data + i * step);
        }

        T const &at (int i) const {
            return *(T const *)(data + i * step);
        }

        T &operator [] (int i) {
            return at(i);
        }

        T const &operator [] (int i) const {
            return at(i);
        }
    };
};

/*
*/

#endif

