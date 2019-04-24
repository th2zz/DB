/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "bad_index_info_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

BadIndexInfoException::BadIndexInfoException(const std::string& reason)
    : BadgerDbException(""), reason_(reason) {
  std::stringstream ss;
  ss << "Bad Index Info read: " << reason_;
  message_.assign(ss.str());
}

}
