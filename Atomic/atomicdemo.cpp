#include <pthread.h>
#include <cstdio>
#include <atomic>

#define MT_LEVEL 5

struct ThreadContext {
    std::atomic<int>* atomic_counter;
    int* bad_counter;
};


void* foo(void* arg)
{
    ThreadContext* tc = (ThreadContext*) arg;
    
    for (int i = 0; i < 1000; ++i) {
        // old_value isn't used in this example, but will be necessary
        // in the exercise
        int old_value = (*(tc->atomic_counter))++;
        (void) old_value;  // ignore not used warning
        (*(tc->bad_counter))++;
    }
    
    return 0;
}


int main(int argc, char** argv)
{
    pthread_t threads[MT_LEVEL];
    ThreadContext contexts[MT_LEVEL];
    std::atomic<int> atomic_counter(0);
    int bad_counter = 0;

    for (int i = 0; i < MT_LEVEL; ++i) {
        contexts[i] = {&atomic_counter, &bad_counter};
    }
    
    for (int i = 0; i < MT_LEVEL; ++i) {
        pthread_create(threads + i, NULL, foo, contexts + i);
    }
    
    for (int i = 0; i < MT_LEVEL; ++i) {
        pthread_join(threads[i], NULL);
    }
    printf("atomic counter: %d\n", atomic_counter.load());
    printf("bad counter: %d\n", bad_counter);
    
    return 0;
}
