/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * ePipe is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * ePipe is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef UTILS_H
#define UTILS_H

#include "common.h"
#include "Logger.h"
#include<cstdlib>
#include<cstring>

typedef boost::posix_time::ptime ptime;

typedef Ndb* SConn;

struct MConn {
  SConn inodeConnection;
  SConn metadataConnection;
};

namespace Utils {
  
  inline static ptime getCurrentTime() {
    //return boost::posix_time::microsec_clock::universal_time()
    return boost::posix_time::microsec_clock::local_time();
  }

  inline static double getTimeDiffInMilliseconds(ptime start, ptime end) {
    boost::posix_time::time_duration diff = end - start;
    return diff.total_microseconds() / 1000.0;
  }

  inline static double getTimeDiffInSeconds(ptime start, ptime end) {
   return getTimeDiffInMilliseconds(start, end) / 1000.0;
  }

  inline static std::string concat(const char* a, const std::string b) {
    std::string buf(a);
    buf.append(b);
    return buf;
  }
  
  inline static std::string to_string(UISet& set) {
    std::stringstream out;
    out << "[";
    unsigned int i = 0;
    for (UISet::iterator it = set.begin(); it != set.end(); ++it, i++) {
      out << *it << (i < set.size() ? "," : "");
    }
    out << "]";
    return out.str();
  }
}

#endif /* UTILS_H */

