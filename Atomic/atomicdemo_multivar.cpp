#include <pthread.h>
#include <cstdio>
#include <atomic>

#define MT_LEVEL 4

struct ThreadContext {
    int threadID;
    std::atomic<uint32_t>* atomic_counter;
};


void* count(void* arg)
{
    ThreadContext* tc = (ThreadContext*) arg;
    
    int n = 1000;
    for (int i = 0; i < n; ++i) {
        if (i % 10 == 0){
            (*(tc->atomic_counter))++;
        }
        if (i % 100 == 0){
            (*(tc->atomic_counter)) += 1 << 16;
        }
    }
    (*(tc->atomic_counter)) += tc->threadID % 2 << 30;
    
    return 0;
}


int main(int argc, char** argv)
{
    pthread_t threads[MT_LEVEL];
    ThreadContext contexts[MT_LEVEL];
    std::atomic<uint32_t> atomic_counter(0);

    for (int i = 0; i < MT_LEVEL; ++i) {
        contexts[i] = {i, &atomic_counter};
    }
    
    for (int i = 0; i < MT_LEVEL; ++i) {
        pthread_create(threads + i, NULL, count, contexts + i);
    }
    
    for (int i = 0; i < MT_LEVEL; ++i) {
        pthread_join(threads[i], NULL);
    }
    // Note that 0b is in the standard only from c++14
    /* printf("atomic counter first 16 bit: %d\n", atomic_counter.load() & (0b1111111111111111)); */
    printf("atomic counter first 16 bit: %d\n", atomic_counter.load() & (0xffff));
    printf("atomic counter next 15 bit: %d\n", atomic_counter.load()>>16 & (0x7fff));
    printf("atomic counter last 2 bit: %d\n", atomic_counter.load()>>30);
    
    return 0;
}
