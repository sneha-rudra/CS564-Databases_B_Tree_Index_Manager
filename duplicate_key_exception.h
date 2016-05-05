/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <string>

#include "badgerdb_exception.h"
#include "types.h"

namespace badgerdb {

/**
 * @brief An exception that is thrown when a duplicate key is being inserted.
 */
class DuplicateKeyException : public BadgerDbException {
 public:
  /**
   * Constructs a duplicate key exception when a key already exists in the index
   */
  DuplicateKeyException();


 protected:

};

}

