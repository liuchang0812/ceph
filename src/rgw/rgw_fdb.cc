// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_fdb.h"
#include "include/assert.h"
#include "boost/algorithm/string/predicate.hpp"

#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/dout.h"
#include "common/safe_io.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw

fdb_error_t waitError(FDBFuture *f) {
    fdb_error_t blockError = fdb_future_block_until_ready(f);
    if(!blockError) {
        return fdb_future_get_error(f);
    } else {
        return blockError;
    }
}

fdb_error_t checkRetry(FDBTransaction* tr, fdb_error_t err) {
  FDBFuture *f = fdb_transaction_on_error(tr, err);
  fdb_error_t retryE = waitError(f);
  fdb_future_destroy(f);
  if (retryE) {
    return retryE;
  }
  return 0;
}

fdb_error_t checkError(fdb_error_t err, const char* context) {
    return err;
}

void* runNetwork(void* arg) {
    checkError(fdb_run_network(), "run network");
    return nullptr;
}

void fdb_global_init() {

    ldout(g_ceph_context, 0) << __func__ << " DEBUGLC "<< dendl;
    fdb_select_api_version(FDB_API_VERSION);
    fdb_error_t err = checkError(fdb_setup_network(), "setup network");
    if (err) {
      ldout(g_ceph_context, 0) << __func__ << " DEBUGLC: " << err << dendl;
        return ;
    }

    pthread_t netThread;
    pthread_create(&netThread, nullptr, &runNetwork, nullptr);
}

struct RunResult openDatabase(FDBDatabase** db) {
    fdb_error_t err;
    FDBFuture *f = fdb_create_cluster(nullptr);
    err = checkError(fdb_future_block_until_ready(f), "block for cluster");
    if (err) {
        return RunResult{-1, err};
    }

    FDBCluster *cluster;
    err = checkError(fdb_future_get_cluster(f, &cluster), "get cluster");
    if (err) {
        return RunResult{-1, err};
    }

    fdb_future_destroy(f);

    f = fdb_cluster_create_database(cluster, (uint8_t*)"DB", 2);

    err = checkError(fdb_future_block_until_ready(f), "block for database");
    if (err) {
        return RunResult{-1, err};
    }

    err = checkError(fdb_future_get_database(f, db), "get database");
    if (err) {
        return RunResult{-1, err};
    }

    fdb_future_destroy(f);
    fdb_cluster_destroy(cluster);

    return RunResult{0, 0};
}

fdb_error_t createTransaction(FDBDatabase* db, FDBTransaction** tr, uint64_t limit, uint64_t timeout) {

    fdb_error_t err = checkError(fdb_database_create_transaction(db, tr), "create transaction");
    if (err) {
        return err;
    }

    err = checkError(fdb_transaction_set_option(*tr, FDB_TR_OPTION_RETRY_LIMIT, (const uint8_t*)&limit, sizeof(uint64_t)), "set retry limit");
    if (err) {
        return err;
    }

    err = checkError(fdb_transaction_set_option(*tr, FDB_TR_OPTION_TIMEOUT, (const uint8_t*)&timeout, sizeof(uint64_t)), "set time out");
    if (err) {
        return err;
    }

    return 0;
}

struct RunResult run(FDBDatabase *db,
		     fdb_error_t (*func)(FDBTransaction*, void*, void*), 
		     void* args,
                     void* result,
                     uint64_t retryLimit,
                     uint64_t timeout)
{
    FDBTransaction *tr = nullptr;
    fdb_error_t err = createTransaction(db, &tr, retryLimit, timeout);

    if (err) {
        return RunResult{-1, err};
    }

    while(1) {
        err = func(tr, args, result);

        if (!err) {
            FDBFuture *f = fdb_transaction_commit(tr);
            err = waitError(f);
            fdb_future_destroy(f);
        }

        if (err) {
            FDBFuture *f = fdb_transaction_on_error(tr, err);
            fdb_error_t retryE = waitError(f);
            fdb_future_destroy(f);
            if (retryE) 
            {
                fdb_transaction_destroy(tr);
                return RunResult{-1, retryE};
            } else {
                continue;
            }

        }
        break;
    }

    fdb_transaction_destroy(tr);
    return RunResult{0, 0};
}

RunResult fdb_put_key_value(FDBDatabase* db, const std::string& key, const std::string& value) {
    FDBTransaction *tr = nullptr;
    fdb_error_t err = createTransaction(db, &tr, MAX_RETRY, MAX_TIMEOUT);

    if (err) 
      return (struct RunResult) {-1, err};

    while (true) {
      fdb_transaction_set(tr, (const uint8_t*)key.c_str(), key.size(), (const uint8_t*)value.c_str(), value.size());

      FDBFuture *f = fdb_transaction_commit(tr);
      err = waitError(f);
      fdb_future_destroy(f);

      if (err) {
	FDBFuture *f = fdb_transaction_on_error(tr, err);
	fdb_error_t retryE = waitError(f);
	fdb_future_destroy(f);
	if (retryE)  {
	  fdb_transaction_destroy(tr);
	  return (struct RunResult) {-1, retryE};
	}
      } else {
	fdb_transaction_destroy(tr);
	return (struct RunResult) {0, err};
      }
    }
    return (struct RunResult) {0, 4100}; // internal_error; we should never get here
}

RunResult fdb_put_key_values(FDBDatabase *db, const keyvalues& kvs) {
    FDBTransaction *tr = nullptr;
    fdb_error_t err = createTransaction(db, &tr, MAX_RETRY, MAX_TIMEOUT);

    if (err) 
      return (struct RunResult) {-1, err};

    while (true) {

      for (auto& kv: kvs) {
	fdb_transaction_set(tr, (const uint8_t*)kv.first.c_str(), kv.first.size(), (const uint8_t*)kv.second.c_str(), kv.second.size());
      }

      FDBFuture *f = fdb_transaction_commit(tr);
      err = waitError(f);
      fdb_future_destroy(f);

      if (err) {
	FDBFuture *f = fdb_transaction_on_error(tr, err);
	fdb_error_t retryE = waitError(f);
	fdb_future_destroy(f);
	if (retryE)  {
	  fdb_transaction_destroy(tr);
	  return (struct RunResult) {-1, retryE};
	}
      } else {
	fdb_transaction_destroy(tr);
	return (struct RunResult) {0, err};
      }
    }
    return (struct RunResult) {0, 4100}; // internal_error; we should never get here
}

RunResult fdb_get_key_value(FDBDatabase *db, const std::string& key, std::string& value, bool& exist) {
    FDBTransaction *tr = nullptr;
    fdb_error_t err = createTransaction(db, &tr, MAX_RETRY, MAX_TIMEOUT);

    if (err) 
      return (struct RunResult) {-1, err};

    while (true) {
      FDBFuture *f = fdb_transaction_get(tr, (const uint8_t*)key.c_str(), key.size(), true);
      err = waitError(f);
      if (!err) {
	int present;
	const uint8_t* raw_value;
	int raw_value_len;
        err = fdb_future_get_value(f, &present, &raw_value, &raw_value_len);
	if (!err) {
	  if (present) {
	    exist = true;
	    value = std::string((char*)raw_value, raw_value_len);
	  } else {
	    exist = false;
	  }
	}
      }
      fdb_future_destroy(f);

      if (err) {
	f = fdb_transaction_on_error(tr, err);
	fdb_error_t retryE = waitError(f);
	fdb_future_destroy(f);
	if (retryE) {
	  fdb_transaction_destroy(tr);
	  return (struct RunResult) {-1, retryE};
	}
      } else {
	fdb_transaction_destroy(tr);
	return (struct RunResult) {0, err};
      }
    }
    return (struct RunResult) {0, 4100}; // internal_error; we should never get here
}

RunResult fdb_rm_key(FDBDatabase *db, const std::string& key) {
    FDBTransaction *tr = nullptr;
    fdb_error_t err = createTransaction(db, &tr, MAX_RETRY, MAX_TIMEOUT);

    if (err) 
      return (struct RunResult) {-1, err};

    while (true) {
      fdb_transaction_clear(tr, (const uint8_t*)key.c_str(), key.size());

      FDBFuture *f = fdb_transaction_commit(tr);
      err = waitError(f);
      fdb_future_destroy(f);

      if (err) {
	FDBFuture *f = fdb_transaction_on_error(tr, err);
	fdb_error_t retryE = waitError(f);
	fdb_future_destroy(f);
	if (retryE)  {
	  fdb_transaction_destroy(tr);
	  return (struct RunResult) {-1, retryE};
	}
      } else {
	fdb_transaction_destroy(tr);
	return (struct RunResult) {0, err};
      }
    }
    return (struct RunResult) {0, 4100}; // internal_error; we should never get here
}

RunResult fdb_list_key_value(FDBDatabase *db, const std::string& startMarker, const std::string& endMarker, int max_count, keyvalues& kvs, bool& truncated) {
  int outCount, totalCount;
  int iteration;
  fdb_bool_t outMore;
  bool getAll;
  const FDBKeyValue *outKv = nullptr;

  FDBTransaction *tr = nullptr;
  fdb_error_t err = createTransaction(db, &tr, MAX_RETRY, MAX_TIMEOUT);
  if (err) {
    return (struct RunResult) {-1, err};
  }
  fdb_error_t retryE;

  while (true) {
    truncated = false;
    getAll = false;
    kvs.clear();
    totalCount = 0;
    iteration = 0;
    outMore = 1;

    FDBFuture *f = fdb_transaction_get_range(tr,
        (const uint8_t *)startMarker.c_str(), (int)startMarker.size(), 1, 1,
        (const uint8_t *)endMarker.c_str(), (int)endMarker.size(), 1, 1, 0, 0,
        FDB_STREAMING_MODE_WANT_ALL, ++iteration, 0, 0);

    while (outMore) {
      err = waitError(f);
      if (err) {
        retryE = checkRetry(tr, err);
        break;
      }

      err = fdb_future_get_keyvalue_array(f, &outKv, &outCount, &outMore);
      if (err) {
        retryE = checkRetry(tr, err);
        break;
      }

      totalCount += outCount;
      for (int i = 0; i < outCount; ++i) {
        kvs.push_back(make_pair(
            std::string((char *)outKv[i].key, outKv[i].key_length),
            std::string((char *)outKv[i].value, outKv[i].value_length)));
      }

      if (outMore) {
        if (totalCount >= max_count) {
          truncated = true;
          getAll = true;
          break;
        }
        FDBFuture *f2 = fdb_transaction_get_range(tr,
            (const uint8_t *)outKv[outCount - 1].key, outKv[outCount - 1].key_length, 1, 1,
            (const uint8_t *)endMarker.c_str(), (int)endMarker.size(), 1, 1, 0,
            0, FDB_STREAMING_MODE_WANT_ALL, ++iteration, 0, 0);
        fdb_future_destroy(f);
        f = f2;
      } else {
        truncated = false;
        getAll = true;
        break;
      }
    }

    fdb_future_destroy(f);
    if (retryE) {
      fdb_transaction_destroy(tr);
      return (struct RunResult){-1, retryE};
    }
    if (getAll) {
      break;
    }
  }

  fdb_transaction_destroy(tr);
  return (struct RunResult){0, 0};
}

RunResult fdb_list_key_value(FDBDatabase *db, const std::string& prefix, const std::string& marker, const std::string& delimiter, int max_count, keyvalues& kvs, bool& truncated) {

    ldout(g_ceph_context, 12) << __func__ << " DEBUGLC: " << prefix << ' ' << marker << ' ' << delimiter << ' ' << dendl;
    kvs.clear();

    std::string marker_ = marker;

    if (!boost::algorithm::starts_with(marker, prefix)) {
      if (marker > prefix) {
	truncated = false;
    	return (struct RunResult) {0, 0};
      } else if (marker < prefix) {
	marker_ = prefix;
      }
    }

    std::string endMarker = prefix.substr(0, prefix.size()-1) + char(prefix[prefix.size()-1] + 1);
    // std::string endMarker = "z";
    fdb_bool_t outMore = 1;
    int iteration = 0;
    int32_t totalOut = 0;
    std::string pre_common_prefix;

    FDBTransaction *tr = nullptr;
    fdb_error_t err = createTransaction(db, &tr, MAX_RETRY, MAX_TIMEOUT);

    if (err) {
      return (struct RunResult) {-1, err};
    }

    while (true) {
    	FDBFuture *f = fdb_transaction_get_range(tr,
	    // FDB_KEYSEL_FIRST_GREATER_THAN((const uint8_t*)marker_.c_str(), (int)marker_.size()),
            (const uint8_t*)marker_.c_str(), (int)marker_.size(), 1, 1, 
	    // FDB_KEYSEL_LAST_LESS_OR_EQUAL((const uint8_t*)endMarker.c_str(), (int)endMarker.size()),
	    (const uint8_t*)endMarker.c_str(), (int)endMarker.size(), 1, 1,
	    0, 0,
	    FDB_STREAMING_MODE_WANT_ALL, ++iteration, 0, 0);

	while (outMore) {
	  bool getAll = false;
	  err = waitError(f);
	  if (!err) {
	    const FDBKeyValue *outKv;
	    int outCount;

	    err = fdb_future_get_keyvalue_array(f, &outKv, &outCount, &outMore);

	    if (!err) {

	      ldout(g_ceph_context, 12) << __func__ << " DEBUGLC: outCount: " << outCount << " outMore " << outMore << dendl;
	      for (int i=0; i<outCount; ++i) {
		// check delimiter 
		std::string key_ = std::string((char*)outKv[i].key, outKv[i].key_length);;
	        ldout(g_ceph_context, 12) << __func__ << " DEBUGLC: found key: " << key_ << dendl;
		// has delimiter ?
		if (key_.substr(prefix.size()).find(delimiter) ==  std::string::npos) {
		  kvs.push_back(make_pair(std::string((char*)outKv[i].key, outKv[i].key_length), std::string((char*)outKv[i].value, outKv[i].value_length)));
		  ++ totalOut;
		  if (totalOut >= max_count) {
		    truncated = true;

		    ldout(g_ceph_context, 12) << __func__ << " DEBUGLCBREAK: : " << dendl;
		    getAll = true;
		    break;
		  }
		} else {
		  std::string common_prefix = key_.substr(0, prefix.size() + key_.substr(prefix.size()).find(delimiter) + 1);
		  if (common_prefix != pre_common_prefix)  {
		    pre_common_prefix = common_prefix;
		    ++ totalOut;
		    kvs.push_back(make_pair(common_prefix, std::string((char*)outKv[i].value, outKv[i].value_length)));
		    if (totalOut >= max_count) {
		      truncated = true;

		      ldout(g_ceph_context, 12) << __func__ << " DEBUGLCBREAK: : " << dendl;

		      getAll = true;
		      break;
		    }
		  }
		}
	      }
	    }

	    if (outMore && !getAll) {

	      ldout(g_ceph_context, 12) << __func__ << " DEBUGLC: iteration " << iteration+1 << ' '  << std::string((char*)outKv[outCount-1].key, outKv[outCount-1].key_length) << ' ' << endMarker<< dendl;
	      FDBFuture *f2 = fdb_transaction_get_range(tr,
		  (const uint8_t*)outKv[outCount-1].key, outKv[outCount-1].key_length, 1, 1,
		  // FDB_KEYSEL_LAST_LESS_OR_EQUAL((const uint8_t*)endMarker.c_str(), (int)endMarker.size()),
		  (const uint8_t*)endMarker.c_str(), (int)endMarker.size(), 1, 1,
		  0, 0,
		  FDB_STREAMING_MODE_WANT_ALL, ++iteration, 0, 0);
	      fdb_future_destroy(f);
	      f = f2;
	    }
	  }
	  if (getAll || err) break;
	}
	fdb_future_destroy(f);

	if (totalOut < max_count) 
	  truncated = false;

	if (err) {
	  f = fdb_transaction_on_error(tr, err);
	  fdb_error_t retryE = waitError(f);
	  fdb_future_destroy(f);
	  if (retryE) {
	    fdb_transaction_destroy(tr);
	    return (struct RunResult) {-1, retryE};
	  }
	} else {
	  if (totalOut < max_count) truncated = false;
	  else truncated = true;
	  fdb_transaction_destroy(tr);
	  return (struct RunResult) {0, err};
	}
    }
}
