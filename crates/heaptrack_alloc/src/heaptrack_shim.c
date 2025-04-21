#include <stddef.h>

#define HEAPTRACK_API_DLSYM 1
#include "heaptrack_api.h"

void ht_report_alloc(void *p, size_t sz)
{
    heaptrack_report_alloc(p, sz);
}

void ht_report_free(void *p)
{
    heaptrack_report_free(p);
}

void ht_report_realloc(void *oldp,
                       size_t newsz,
                       void *newp)
{
    heaptrack_report_realloc(oldp, newsz, newp);
}