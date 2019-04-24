/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */


#pragma once

#include <string>
#include "types.h"
#include "page.h"
#include "buffer.h"
#include "file_iterator.h"
#include "page_iterator.h"

namespace badgerdb {

/**
 * @brief This class is used to sequentially scan records in a relation.
 */
class FileScan
{
 public:

  FileScan(const std::string &name, BufMgr *bufMgr);

  ~FileScan();

  //return RecordId of next record that satisfies the scan 
  void scanNext(RecordId& outRid);

  //read current record, returning pointer and length
  std::string getRecord();

  //marks current page of scan dirty
  void markDirty();

 private:
  /**
   * File which is being scanned.
   */
  PageFile      *file;

  /**
   * Buffer Manager instance used to read/write pages into/from buffer pool.
   */
	BufMgr				*bufMgr;

  /**
   * Current page being scanned.
   */
  Page*         curPage;

  FileIterator  filePageIter;
  PageIterator  pageRecordIter;

  /**
   * True if page has been updated
   */
  bool  	      curDirtyFlag;
};

}
