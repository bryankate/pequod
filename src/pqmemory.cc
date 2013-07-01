#include "pqmemory.hh"
#include <cstdlib>

namespace pq {

uint64_t mem_overhead_size = 0;
uint64_t mem_other_size = 0;
uint64_t mem_store_size = 0;

namespace {
struct meminfo {
    uint64_t* type;
    size_t sz;
};
}

void* allocate(size_t sz, uint64_t* type) {
    if (sz == 0)
        return NULL;
    if (!enable_memory_tracking)
        return malloc(sz);

    size_t xsz = sz + sizeof(meminfo);
    meminfo* mi;
    if (xsz < sz || !(mi = (meminfo*)malloc(xsz)))
	return NULL;
    mi->type = type;
    mi->sz = sz;
    mem_overhead_size += xsz - sz;
    if (type)
        *type += sz;
    else
        mem_other_size += sz;
    return (void *) (mi + 1);
}

void deallocate(void* p) {
    if (!p)
        return;
    if (!enable_memory_tracking) {
        free(p);
        return;
    }

    meminfo* mi = (meminfo*)p - 1;
    mem_overhead_size -= sizeof(meminfo);
    if (mi->type)
        *(mi->type) -= mi->sz;
    else
        mem_other_size -= mi->sz;
    free(mi);
}

} // namepace pq

void* operator new(size_t size) {
    return pq::allocate(size, nullptr);
}

void* operator new[](size_t size) {
    return pq::allocate(size, nullptr);
}

void* operator new(size_t size, uint64_t* type) {
    return pq::allocate(size, type);
}

void* operator new[](size_t size, uint64_t* type) {
    return pq::allocate(size, type);
}

void operator delete(void *p) {
    pq::deallocate(p);
}

void operator delete[](void *p) {
    pq::deallocate(p);
}

