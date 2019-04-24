/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "no_such_key_found_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

NoSuchKeyFoundException::NoSuchKeyFoundException()
    : BadgerDbException(""){
  std::stringstream ss;
  ss << "No such key found.";
  message_.assign(ss.str());
}

}
