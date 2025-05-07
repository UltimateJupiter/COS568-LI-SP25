#ifndef TLI_DYNAMIC_PGM_HYBRID_OPT_H
#define TLI_DYNAMIC_PGM_HYBRID_OPT_H
#pragma once

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <functional>
#include <shared_mutex>
#include <condition_variable>

#include "../util.h"
#include "dynamic_pgm_index.h"
#include "lipp.h"

using namespace std;
bool verbose = false;
bool debug = false;

template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGM_LIPP_OPT : public Competitor<KeyType, SearchClass> {
public:
    using DPGM_Type = DynamicPGM<KeyType, SearchClass, pgm_error>;
    using LIPP_Type = Lipp<KeyType>;

    // Initialize DPGM
    DPGM_Type pgm1;
    DPGM_Type pgm2;

    // Initialize LIPP
    LIPP_Type lipp1;
    LIPP_Type lipp2;

    HybridPGM_LIPP_OPT(const std::vector<int>& params)
        : pgm1(params), pgm2(params), 
          lipp1(params), lipp2(params),
          total_size(0), pgm1_size(0), pgm2_size(0),
          rw_pgm_ptr(&pgm1), r_pgm_ptr(&pgm2),
          w_lipp_ptr(&lipp1), r_lipp_ptr(&lipp2) {
        // Initialize everything in constructor
        flush_complete.store(true);
        lookup_insert_ok.store(true);
        read_lipp_ok.store(true);
        should_stop_flush.store(false);
    }

    ~HybridPGM_LIPP_OPT() {
        // Ensure flush thread is finished before destruction
        stopFlushThread();
    }

    uint64_t Build(const std::vector<KeyValue<KeyType>>& data, size_t num_threads) {
        total_size = data.size();
        uint64_t build_time1 = lipp1.Build(data, num_threads);
        uint64_t build_time2 = lipp2.Build(data, num_threads);
        return build_time1 + build_time2;
    }

    size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
        
        if (verbose) {cout << "Looking up key in DPGM: " << lookup_key << endl;}
        uint64_t value = util::OVERFLOW;

        if (pgm1_size.load() == 0 && pgm2_size.load() == 0) {
            // Directly read from LIPP
            while (!read_lipp_ok.load()) {
                std::this_thread::yield();
            }
            if (r_lipp_ptr != nullptr) {
                value = r_lipp_ptr->EqualityLookup(lookup_key, thread_id);
            }
            return value;
        }
        
        
        // Wait until lookup is safe
        while (!lookup_insert_ok.load()) {
            std::this_thread::yield();
        }
        
        // Use shared_lock for concurrent reads
        shared_lock<shared_mutex> read_lock(pgm_mutex);
        if (rw_pgm_ptr != nullptr) {
            value = rw_pgm_ptr->EqualityLookup(lookup_key, thread_id);
        }
        read_lock.unlock();
        
        if (value == util::OVERFLOW) {

            // Fall back to read-only PGM
            if (r_pgm_ptr != nullptr) {
                value = r_pgm_ptr->EqualityLookup(lookup_key, thread_id);
            }
            
            if (value == util::OVERFLOW) {
                // Wait until LIPP read is safe
                while (!read_lipp_ok.load()) {
                    std::this_thread::yield();
                }
                
                if (r_lipp_ptr != nullptr) {
                    value = r_lipp_ptr->EqualityLookup(lookup_key, thread_id);
                }
            }
        }

        return value;
    }

    uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
        shared_lock<shared_mutex> read_lock(pgm_mutex);
        uint64_t result = 0;
        if (rw_pgm_ptr != nullptr) {
            result = rw_pgm_ptr->RangeQuery(lower_key, upper_key, thread_id);
        }
        return result;
    }

    void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
        // Wait until insert is safe
        if (verbose) {cout << "Inserting data to DPGM: " << data.key << endl;}

        while (!lookup_insert_ok.load()) {
            std::this_thread::yield();
        }
        
        {
            // Lock for writing to PGM
            unique_lock<shared_mutex> write_lock(pgm_mutex);
            if (rw_pgm_ptr != nullptr) {
                rw_pgm_ptr->Insert(data, thread_id);
            }
            
            // Atomically increment sizes
            size_t new_pgm1_size = ++pgm1_size;
            size_t new_total_size = ++total_size;
            // cout << "New PGM1 size: " << new_pgm1_size << ", Total size: " << new_total_size << endl;
            // Check if we need to flush
            if (new_pgm1_size >= 0.002 * new_total_size) {
                // Check if a flush is already in progress
                if (flush_complete.load()) {
                    // Try to initiate flush
                    lock_guard<mutex> flush_lock(flush_mutex);
                    // Check condition again to avoid race condition
                    if (flush_complete.load()) {
                        initiateFlush();
                    }
                }
            }
        }
    }

    std::string name() const { return "HybridPGMLIPP_OPT_0.002"; }

    std::size_t size() const { 
        size_t total = 0;
        shared_lock<shared_mutex> lock1(pgm_mutex);
        shared_lock<shared_mutex> lock2(lipp_mutex);
        total += pgm1.size() + pgm2.size() + lipp1.size() + lipp2.size();
        return total;
    }

    bool applicable(bool unique, bool range_query, bool insert, bool multithread, const std::string& ops_filename) const {
        std::string name = SearchClass::name();
        return name != "LinearAVX" && unique;
    }

    std::vector<std::string> variants() const {
        return std::vector<std::string>();
    }

private:
    size_t total_size;
    atomic<size_t> pgm1_size;
    atomic<size_t> pgm2_size;
    int reading_count = 0;

    // Use raw pointers with proper synchronization
    DPGM_Type* rw_pgm_ptr;
    DPGM_Type* r_pgm_ptr;
    
    LIPP_Type* w_lipp_ptr;
    LIPP_Type* r_lipp_ptr;

    // Mutexes for synchronization
    mutable shared_mutex pgm_mutex;   // For read/write to pgm pointers
    mutable shared_mutex lipp_mutex;  // For read/write to lipp pointers
    mutex flush_mutex;                // For controlling flush operations
    
    // Atomic flags for state management
    atomic<bool> read_lipp_ok;
    atomic<bool> lookup_insert_ok;
    atomic<bool> flush_complete;
    atomic<bool> should_stop_flush;
    
    // For flush thread management
    thread flush_thread;
    condition_variable flush_cv;
    mutex flush_thread_mutex;

    void initiateFlush() {
        if (debug) { cout << "Initiating flush..." << endl; }
        
        // Set flag to indicate flush is in progress
        flush_complete.store(false);
        
        // Temporarily disable lookups/inserts
        lookup_insert_ok.store(false);
        
        // Swap the pgm pointers
        DPGM_Type* tmp_ptr = rw_pgm_ptr;
        rw_pgm_ptr = r_pgm_ptr;
        r_pgm_ptr = tmp_ptr;
        
        // Swap the pgm sizes
        size_t tmp_size = pgm1_size.load();
        pgm1_size.store(pgm2_size.load());
        pgm2_size.store(0);
        
        // Re-enable lookups/inserts
        lookup_insert_ok.store(true);
        
        // Start the flush thread
        startFlushThread();
    }

    void startFlushThread() {
        // Stop any existing flush thread
        stopFlushThread();
        
        // Create a new flush thread
        {
            lock_guard<mutex> lock(flush_thread_mutex);
            flush_thread = std::thread(&HybridPGM_LIPP_OPT::flush, this);
        }
    }
    
    void stopFlushThread() {
        should_stop_flush.store(true);
        
        {
            lock_guard<mutex> lock(flush_thread_mutex);
            if (flush_thread.joinable()) {
                flush_thread.join();
            }
        }
        
        should_stop_flush.store(false);
    }

    void flush() {
        // cout << "Flushing thread started" << endl;
        
        // Use a unique lock for the entire flush operation
        unique_lock<mutex> flush_lock(flush_mutex);
        
        // Extract data from r_pgm_ptr with proper synchronization
        std::vector<KeyValue<KeyType>> pgm_cache;
        {
            shared_lock<shared_mutex> read_lock(pgm_mutex);
            if (r_pgm_ptr != nullptr) {
                pgm_cache = r_pgm_ptr->extract_data();
            }
        }
        
        // Copy data to the write LIPP
        {
            int progress = 0;

            unique_lock<shared_mutex> write_lock(lipp_mutex);
            for (const auto& kv : pgm_cache) {
                if (should_stop_flush.load()) {
                    // cout << "Flush stopped prematurely" << endl;
                    return;
                }
                if (w_lipp_ptr != nullptr) {
                    w_lipp_ptr->Insert(KeyValue<KeyType>{kv.key, kv.value}, 0);
                }
                if (verbose) { cout << "LIPP1 writing Progress: " << ++progress << endl; }
            }
        }
        
        // Block read from LIPP while swapping
        read_lipp_ok.store(false);
        
        // Swap LIPP pointers
        {
            unique_lock<shared_mutex> write_lock(lipp_mutex);
            LIPP_Type* tmp_ptr = w_lipp_ptr;
            w_lipp_ptr = r_lipp_ptr;
            r_lipp_ptr = tmp_ptr;
        }
        
        // Re-enable LIPP reading
        read_lipp_ok.store(true);
        
        // Copy data to the new write LIPP (was read LIPP)
        {
            int progress = 0;
            
            unique_lock<shared_mutex> write_lock(lipp_mutex);
            for (const auto& kv : pgm_cache) {
                if (should_stop_flush.load()) {
                    if (debug) { cout << "Flush stopped prematurely" << endl; }
                    return;
                }
                if (w_lipp_ptr != nullptr) {
                    w_lipp_ptr->Insert(KeyValue<KeyType>{kv.key, kv.value}, 0);
                }
                if (verbose) { cout << "LIPP2 writing Progress: " << ++progress << endl; }
            }
        }
        
        // Clear the old PGM that was flushed
        {
            unique_lock<shared_mutex> write_lock(pgm_mutex);
            if (r_pgm_ptr != nullptr) {
                r_pgm_ptr->clear();
            }
        }
        
        // Flush complete
        flush_complete.store(true);
        // cout << "Flush complete" << endl;
    }
};

#endif