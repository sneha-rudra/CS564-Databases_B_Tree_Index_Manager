/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "duplicate_key_exception.h"

#include <sstream>
#include <string>

namespace badgerdb {

DuplicateKeyException::DuplicateKeyException() : BadgerDbException(""){
  std::stringstream ss;
  ss << "Attempting to insert duplicate key";
  message_.assign(ss.str());
}

}
