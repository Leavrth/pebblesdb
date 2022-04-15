#ifndef PMEM_KCACHE_H_
#define PMEM_KCACHE_H_

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/transaction.hpp>

using namespace pmem;
using namespace pmem::obj;
#include <cstdlib>
#include <iostream>
#include <unistd.h>

namespace kcache { 

#define NBUCKETS 1007
#define LAYOUT_NAME "kcache"
#define POOL_SIZE ((size_t)128 * 1024 * 1024)

struct hash_entry {

    /** Key-Value, value is the filenumber of the SSTable the key exists in */
    persistent_ptr<string>  key;
    p<int>                  val;

    /** Next Entry, wrapped by persistent_pool_ptr */
    persistent_ptr<hash_entry> next_entry;
};

/** Pool root structure */
struct root {
    persistent_ptr<hash_entry> buckets[NBUCKETS];
};

class kcache {
public:
    explicit kcache(const char *path) {
        if (access(path, F_OK) == 0) {
            try {
                pop = pool<root>::open(path, LAYOUT_NAME);
            } catch (const pool_error &e) {
                // Pool not found
                std::cerr << "open pmem pool failed. " << e.what() << std::endl;
                exit(1);
            }
        } else {
            try {
                pop = pool<root>::create(path, LAYOUT_NAME, POOL_SIZE, S_IWUSR | S_IRUSR);
            } catch (const pool_error &e) {
                std::cerr << "create pmem pool failed. " << e.what() << std::endl;
                exit(1);
            }
        }
    }

    ~kcache() {
        try {
            pop.close();
        } catch (const std::logic_error &e) {
            std::cerr << "close pmem pool failed. " << e.what() << std::endl;
        }
    }

    int insert(const char *key, int klen, int filenumber) {
        persistent_ptr<root> r = pop.root();
        persistent_ptr<hash_entry> entry;
        uint h = bkdr_hash(key, klen);

        for (entry = r->buckets[h];
             entry != nullptr;
             entry = entry->next_entry) {
            
            if (strncmp(key, entry->key->c_str(), klen) == 0) {
                transaction::run(pop, [&] {
                    entry->val = filenumber;
                });

                return 1;
            }
        }

        transaction::run(pop, [&] {
            persistent_ptr<hash_entry> entry = pmemobj_tx_alloc(sizeof(hash_entry), 0);
            entry->next_entry = r->buckets[h];
            entry->key = make_persistent<string>(key, klen);
            entry->val = filenumber;
            r->buckets[h] = entry;
        });

        return 0;
    }

    /** try replace the key. Return 0 if the key is not found. Otherwise, return -1 if assert_filenumber is different from
     * the filenumber of the cached key. */
    int replace(const char *key, int klen, int assert_filenumber, int update_filenumber) {
        persistent_ptr<root> r= pop.root();
        persistent_ptr<hash_entry> entry;
        uint h = bkdr_hash(key, klen);

        for (entry = r->buckets[h];
             entry != nullptr;
             entry = entry->next_entry) {

            if (strncmp(key, entry->key->c_str(), klen) == 0) {
                if (entry->val == assert_filenumber) {
                    transaction::run(pop, [&] {
                        entry->val = update_filenumber;
                    });

                    return 1;
                }
                // different filenumber
                return -1;
            }      
        }

        // not found
        return 0;
    }

    int get(const char *key, int klen) {
        persistent_ptr<root> r = pop.root();
        persistent_ptr<hash_entry> entry;
        uint h = bkdr_hash(key, klen);

        for (entry = r->buckets[h];
             entry != nullptr;
             entry = entry->next_entry) {
            
            if (strncmp(key, entry->key->c_str(), klen) == 0) {

                return entry->val;
            }
        }
        
        return -1;
    }

private:
    uint bkdr_hash(const char *key, int klen) {
        register uint hash = 0;
        for (int i = 0; i < klen; ++i)
            hash = hash * 131 + (uint)key[i];
        return hash % NBUCKETS;
    }
    pool<root> pop;

    /** support for concurrent kcache */
    std::mutex mutexs[NBUCKETS];
};

}

#endif