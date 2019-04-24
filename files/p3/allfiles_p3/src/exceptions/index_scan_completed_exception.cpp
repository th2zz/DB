/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "index_scan_completed_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

IndexScanCompletedException::IndexScanCompletedException()
    : BadgerDbException(""){
  std::stringstream ss;
  ss << "Index Scan Completed";
  message_.assign(ss.str());
}

}
