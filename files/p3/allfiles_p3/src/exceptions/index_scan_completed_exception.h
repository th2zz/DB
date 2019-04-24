/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <string>

#include "badgerdb_exception.h"

namespace badgerdb {

/**
 * @brief An exception that is thrown when a file operation is requested for a
 *        filename that doesn't exist.
 */
class IndexScanCompletedException : public BadgerDbException {
 public:
  /**
   * Constructs a file not found exception for the given file.
   */
  IndexScanCompletedException();
};

}
