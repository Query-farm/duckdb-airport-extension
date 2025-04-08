#pragma once
#ifndef CURLPOOL_H
#define CURLPOOL_H

#include <curl/curl.h>
#include <vector>
#include <mutex>
#include <condition_variable>

namespace duckdb
{
  class AirportCurlPool
  {
  public:
    // Constructor: Initializes a pool with the given number of CURL handles
    explicit AirportCurlPool(size_t size);

    // Destructor: Cleans up all CURL handles in the pool
    ~AirportCurlPool();

    // Acquire a CURL handle from the pool
    CURL *acquire();

    // Release a CURL handle back to the pool
    void release(CURL *handle);

  private:
    std::vector<CURL *> _pool;   // Pool of CURL handles
    std::mutex _mutex;           // Mutex for synchronizing access to the pool
    std::condition_variable _cv; // Condition variable for signaling availability of handles
  };

}

#endif // CURLPOOL_H
