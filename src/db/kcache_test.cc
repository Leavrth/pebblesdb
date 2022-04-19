#include "kcache.hpp"
#include "util/testharness.h"

namespace leveldb {

#define PMEM_PATH(x) "/mnt/mem/"#x
class KcacheTest { };

TEST(KcacheTest, NORMAL) {
    kcache::kcache kcache(PMEM_PATH(kcache_normal));

    ASSERT_EQ(kcache.insert("key0", 4, 1), 2);
    ASSERT_EQ(kcache.insert("key0", 4, 1), 0);
    ASSERT_EQ(kcache.insert("key0", 4, 2), 0);

    ASSERT_EQ(kcache.replace("key1", 4, 2, 3), 0);
    ASSERT_EQ(kcache.replace("key0", 4, 1, 3), -1);
    ASSERT_EQ(kcache.replace("key0", 4, 2, 3), 1);

    ASSERT_EQ(kcache.get("key0", 4), 3);
    ASSERT_EQ(kcache.get("key1", 4), -1);
}

TEST(KcacheTest, REPLACEMENT) {
    kcache::kcache kcache(PMEM_PATH(kcache_replacement));
    // These keys are all hashed to 415 by hash function kcache::kcache::bkdr_hash.
    ASSERT_EQ(kcache.insert("8_000149", 8, 14), 2);
    ASSERT_EQ(kcache.insert("8_00004c", 8, 8), 2);
    ASSERT_EQ(kcache.insert("7_000460", 8, 4), 2);
    ASSERT_EQ(kcache.insert("7_0000ee", 8, 6), 2);
    ASSERT_EQ(kcache.insert("6_0004c7", 8, 1), 2);
    ASSERT_EQ(kcache.insert("6_0003ca", 8, 7), 2);
    ASSERT_EQ(kcache.insert("6_0001d2", 8, 13), 2);
    ASSERT_EQ(kcache.insert("4_00027d", 8, 9), 2);
    ASSERT_EQ(kcache.insert("4_000104", 8, 2), 2);
    ASSERT_EQ(kcache.insert("4_000085", 8, 3), 2);
    // ten entries
    ASSERT_EQ(kcache.get("6_0004c7", 8), 1);
    ASSERT_EQ(kcache.insert("3_0001a6", 8, 10), 1);
    ASSERT_EQ(kcache.get("6_0004c7", 8), -1);

    ASSERT_EQ(kcache.get("4_000104", 8), 2);
    ASSERT_EQ(kcache.insert("2_0005fb", 8, 4), 1);
    ASSERT_EQ(kcache.get("4_000104", 8), -1);

}
}

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
