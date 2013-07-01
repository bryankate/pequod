#ifndef PQ_MEMORY_HH_
#define PQ_MEMORY_HH_ 1

#include "compiler.hh"
#include <memory>
#include <cstddef>

void* operator new(size_t, int64_t* type);
void* operator new[](size_t, int64_t* type);

namespace pq {

void* allocate(size_t, uint64_t* type);
void deallocate(void* p);

enum { enable_memory_tracking = 1 };

template <typename T, uint64_t* mem_type = nullptr>
class Allocator {
  public:
    typedef T*          pointer;
    typedef const T*    const_pointer;
    typedef T&          reference;
    typedef const T&    const_reference;
    typedef T           value_type;
    typedef size_t      size_type;
    typedef ptrdiff_t   difference_type;

    inline pointer allocate(size_type n, const void* = 0) {
        return reinterpret_cast<pointer>(pq::allocate(n* sizeof(T), mem_type));
    }

    inline void deallocate(pointer p, size_type) {
        pq::deallocate(p);
    }

    inline size_type max_size() const throw() {
        return size_type(-1) / sizeof(T);
    }

    inline pointer address(reference t) const {
        return std::__addressof(t);
    }

    inline const_pointer address(const_reference t) const {
        return std::__addressof(t);
    }

    inline void destroy(pointer p) {
        p->~T();
    }

    template <class... Args >
    inline void construct(pointer p, Args&&... args) {
        ::new((void*)p) T(std::forward<Args>(args)...);
    }

    template <class U>
    struct rebind {
        typedef Allocator<U> other;
    };
};


extern uint64_t mem_overhead_size;
extern uint64_t mem_other_size;
extern uint64_t mem_store_size;

template <class T>
struct heap_type {
    typedef pq::Allocator<T, &mem_store_size> store;
};


// report rusage ru_maxrss in megabytes
inline uint32_t maxrss_mb(int32_t rss) {
#ifdef __APPLE__
    return (uint32_t)rss >> 20; // apple reports in bytes
#else
    return (uint32_t)rss >> 10; // linux reports in kilobytes
#endif
}

}

#endif
