/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "scan_not_initialized_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

ScanNotInitializedException::ScanNotInitializedException()
    : BadgerDbException(""){
  std::stringstream ss;
  ss << "Scan Not Initialized";
  message_.assign(ss.str());
}

}
