/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "bad_scan_param_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

BadScanParamException::BadScanParamException()
    : BadgerDbException(""){
  std::stringstream ss;
  ss << "Bad scan parameters provided.";
  message_.assign(ss.str());
}

}
