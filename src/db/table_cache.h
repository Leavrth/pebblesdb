// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <string>
#include <stdint.h>
#include <unordered_map>
#include "db/dbformat.h"
#include "pebblesdb/cache.h"
#include "pebblesdb/table.h"
#include "port/port.h"
#include "util/timer.h"
#include "db/version_set.h"
#include "pebblesdb/iterator.h"
//#include <unordered_map>

namespace leveldb {

class Env;

class TableCache {
 public:
  TableCache(const std::string& dbname, const Options* options, int entries);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-NULL, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or NULL if no Table object underlies
  // the returned iterator.  The returned "*tableptr" object is owned by
  // the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  Iterator* NewIterator(const ReadOptions& options,
                        uint64_t file_number,
                        uint64_t file_size,
                        Table** tableptr = NULL);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  Status Get(const ReadOptions& options,
             uint64_t file_number,
             uint64_t file_size,
             const Slice& k,
             void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&),
			 Timer* timer);

  // Evict any entry for the specified file number
  void Evict(uint64_t file_number);

  void SetFileMetaDataMap(uint64_t file_number, uint64_t file_size, InternalKey smallest, InternalKey largest);

  FileMetaData* GetFileMetaDataForFile(uint64_t file_number) {
	  if (file_metadata_map.count(file_number) > 0) {
		  return file_metadata_map[file_number];
	  }
	  return NULL;
  }

  void RemoveFileMetaDataMapForFile(uint64_t number) {
	  if (file_metadata_map.count(number) > 0) {
		  FileMetaData* f = file_metadata_map[number];
		  if (f != NULL) {
			  delete f;
		  }
		  file_metadata_map.erase(number);
	  }
  }
  Timer* static_timers_[NUM_SEEK_THREADS];

  void PrintSeekThreadsStaticTimerAuditIndividual() {
	  for (int i = 0; i < NUM_SEEK_THREADS; i++) {
	  	  printf("-------------------------- Individual static timer information %d --------------------\n", i);
	  	  printf("%s\n", static_timers_[i]->DebugString().c_str());
	  	  printf("-------------------------------------------------------------------\n");
	  }
  }

  void PrintSeekThreadsStaticTimerAuditCumulative() {
	  Timer* cum_timer = new Timer();
	  for (int i = 0; i < NUM_SEEK_THREADS; i++) {
		  cum_timer->AppendTimerInfo(static_timers_[i]);
	  }
	  printf("-------------------------- Cumulative static timer information --------------------\n");
	  printf("%s\n", cum_timer->DebugString().c_str());
	  printf("-------------------------------------------------------------------\n");
	  delete cum_timer;
  }


 private:
  TableCache(const TableCache&);
  TableCache& operator = (const TableCache&);
  Env* const env_;
  const std::string dbname_;
  const Options* options_;
  Cache* cache_;
  std::map<uint64_t, FileMetaData*> file_metadata_map;
//  std::unordered_map<uint64_t, Cache::Handle*> cache_handle_map;

  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**, Timer* timer);
};


// A internal wrapper class with an interface similar to Iterator that
// save the filenumber it's from.
class IteratorFilenumber: public Iterator {
 public:
  explicit IteratorFilenumber(Iterator* it, int filenumber) : filenumber_(filenumber), iter_(NULL), valid_(false) {
    Set(it);
  }
  ~IteratorFilenumber() { if (iter_ != NULL) delete iter_; }
  Iterator* iter() const { return iter_; }

  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.
  void Set(Iterator* it) {
    if (iter_ != NULL)
      delete iter_;
    iter_ = it;
    if (iter_ != NULL) {
      Update();
    }
  }


  // Iterator interface methods
  virtual bool Valid() const        { return valid_; }
  virtual Slice key() const         { assert(Valid()); return iter_->key(); }
  virtual Slice value() const       { assert(Valid()); return iter_->value(); }
  virtual int filenumber() const  { assert(Valid()); return filenumber_; }
  // Methods below require iter() != NULL
  virtual const Status& status() const { assert(iter_); return iter_->status(); }
  virtual void Next()               { assert(iter_); iter_->Next();        Update(); }
  virtual void Prev()               { assert(iter_); iter_->Prev();        Update(); }
  virtual void Seek(const Slice& k) { assert(iter_); iter_->Seek(k);       Update(); }
  virtual void SeekToFirst()        { assert(iter_); iter_->SeekToFirst(); Update(); }
  virtual void SeekToLast()         { assert(iter_); iter_->SeekToLast();  Update(); }
  void MakeInvalid()		{ valid_ = false; }

 private:
  IteratorFilenumber();
  IteratorFilenumber(const IteratorFilenumber&);
  IteratorFilenumber& operator = (const IteratorFilenumber&);

  void Update() { valid_ = iter_->Valid(); }

  Iterator* iter_;
  bool valid_;
  int filenumber_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
