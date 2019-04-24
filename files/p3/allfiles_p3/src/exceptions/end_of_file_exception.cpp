/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "end_of_file_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

EndOfFileException::EndOfFileException()
    : BadgerDbException(""){
  std::stringstream ss;
  ss << "End of File reached.";
  message_.assign(ss.str());
}

}
