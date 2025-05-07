#ifndef TLI_DYNAMIC_PGM_HYBRID_H
#define TLI_DYNAMIC_PGM_HYBRID_H
#pragma once

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "../util.h"
#include "base.h"
#include "pgm_index_dynamic.hpp"
#include "./lipp/src/core/lipp.h"

using namespace std;

template <class KeyType, class SearchClass, size_t pgm_error>
class HybridPGM_LIPP : public Competitor<KeyType, SearchClass> {
public:
    HybridPGM_LIPP(const vector<int>& params){}
    uint64_t Build(const vector<KeyValue<KeyType>>& data, size_t num_threads) {
        vector<pair<KeyType, uint64_t>> loading_data;
        
        loading_data.reserve(data.size());
        for (const auto& itm : data) {
            loading_data.push_back(make_pair(itm.key, itm.value));
            lipp_size++;
        }

        uint64_t build_time =
            util::timing([&] {
                // Initialize LIPP
                lipp_.bulk_load(loading_data.data(), loading_data.size());
                // Initialize empty PGM
                pgm_ = decltype(pgm_)();
            });
        
        flush_threshold = lipp_size * pgm_capacity_ratio;
        
        cout << "LIPP build time: " << build_time << endl;
        cout << "HybridPGM_LIPP lipp size: " << lipp_size << endl;
        cout << "Flush threshold: " << flush_threshold << endl;
        // cout << "HybridPGM_LIPP pgm size: " << pgm_.size_in_bytes() << endl;

        return build_time;
    }

    size_t EqualityLookup(const KeyType& lookup_key, uint32_t thread_id) const {
        
        uint64_t value;

        // If the buffer is empty, only check LIPP
        if (pgm_buffer_size == 0) {
            if (lipp_.find(lookup_key, value)) {
                return value;
            }
            else {
                return util::NOT_FOUND;
            }
        }

        // If the buffer is not empty, check DPGM first
        auto it = pgm_.find(lookup_key);
        uint64_t guess;
        if (it != pgm_.end()) {
            guess = it->value();
            return guess;
        }
        else {
            // If not found in DPGM, check LIPP
            if (lipp_.find(lookup_key, value)) {
                guess = value;
            }
            else {
                guess = util::NOT_FOUND;
            }
        }
        return guess;
    }

    uint64_t RangeQuery(const KeyType& lower_key, const KeyType& upper_key, uint32_t thread_id) const {
        
        cout << "Range query: " << lower_key << " " << upper_key << endl;
        
        uint64_t result = 0;
        // Check PGM first
        auto it = pgm_.lower_bound(lower_key);
        while(it != pgm_.end() && it->key() <= upper_key){
            result += it->value();
            ++it;
        }
        
        // Check LIPP
        auto it_lipp = lipp_.lower_bound(lower_key);
        while(it_lipp != lipp_.end() && it_lipp->comp.data.key <= upper_key){
            result += it_lipp->comp.data.value;
            ++it_lipp;
        }

        return result;
    }

    void Insert(const KeyValue<KeyType>& data, uint32_t thread_id) {
        // Insert into DPGM
        pgm_.insert(data.key, data.value);
        pgm_buffer_size++;

        // cout << "PGM_Size: " << pgm_buffer_size << endl;
        // cout << "Capacity: " << flush_threshold << endl;

        // Check size of PGM, flush if needed
        if (pgm_buffer_size >= flush_threshold) {
            cout << "Flushing PGM" << endl;
            // Flush the cache to LIPP
            Flush();
            cout << "Flushed PGM" << endl;
        }
    }
        

    void Flush() {
        cout << "Flushing PGM" << endl;

        // Iterate through PGM and send to cache vector
        vector<pair<KeyType, uint64_t>> pgm_cache_;
        pgm_cache_.reserve(pgm_.size_in_bytes());

        cout << "Creating PGM cache" << endl;
        cout << "LIPP size: " << lipp_size << endl;

        auto it_pgm = pgm_.lower_bound(numeric_limits<KeyType>::lowest());
        while(it_pgm != pgm_.end()){
            pgm_cache_.emplace_back(it_pgm->key(), it_pgm->value());
            ++it_pgm;
        }

        // Insert all items in the pgm_cache_ into LIPP
        // cout << "Inserting PGM cache into LIPP" << endl;
        for (const auto& item : pgm_cache_) {
            lipp_.insert(item.first, item.second);
        }

        // Update the stats for LIPP
        lipp_size += pgm_buffer_size;
        flush_threshold = lipp_size * pgm_capacity_ratio;
        
        // Reset the PGM
        pgm_ = decltype(pgm_)();
        pgm_buffer_size = 0;

        cout << "PGM size after reset: " << pgm_.size_in_bytes() << endl;  
    }

    string name() const { return "HybridPGM_LIPP"; }

    size_t size() const { return pgm_.size_in_bytes() + lipp_.index_size(); }

    bool applicable(bool unique, bool range_query, bool insert, bool multithread, const string& ops_filename) const {
        string name = SearchClass::name();
        return name != "LinearAVX" && !multithread;
    }

    vector<string> variants() const {
        vector<string> vec;
        vec.push_back(SearchClass::name());
        vec.push_back(to_string(pgm_error));
        return vec;
    }

private:
    // Initialize LIPP
    LIPP<KeyType, uint64_t> lipp_;

    // Initialize DPGM
    DynamicPGMIndex<KeyType, uint64_t, SearchClass, PGMIndex<KeyType, SearchClass, pgm_error, 16>> pgm_;
    
    // Pre-set capacity for PGM
    float pgm_capacity_ratio = 0.005;

    // Tracking buffer size
    size_t flush_threshold = 0;
    size_t pgm_buffer_size = 0;
    size_t lipp_size = 0;
};

#endif  // TLI_DYNAMIC_PGM_H
