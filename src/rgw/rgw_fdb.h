#pragma once

#include <pthread.h>
#include <string>
#include <vector>
#include <utility>

// fdb headers
#define FDB_API_VERSION 510
#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>

const uint64_t MAX_RETRY = 0xffffffffffffffff;
const uint64_t MAX_TIMEOUT = 5 * 1000;

fdb_error_t waitError(FDBFuture *f);

fdb_error_t checkError(fdb_error_t err, const char* context);

struct RunResult {
	int res;
	fdb_error_t e;
};

void* runNetwork(void* arg);


void fdb_global_init();
struct RunResult openDatabase(FDBDatabase** db);

fdb_error_t createTransaction(FDBDatabase* db, FDBTransaction** tr, uint64_t limit, uint64_t timeout);

struct RunResult run(FDBDatabase *db,
                     fdb_error_t (*func)(FDBTransaction*, void*, void*),
                     void* args,
                     void* result,
                     uint64_t retryLimit = MAX_RETRY,
                     uint64_t timeout = MAX_TIMEOUT);

typedef std::vector<std::pair<std::string, std::string>> keyvalues;

RunResult fdb_put_key_value(FDBDatabase *db, const std::string& key, const std::string& value);
RunResult fdb_put_key_values(FDBDatabase *db, const keyvalues& kvs);
RunResult fdb_get_key_value(FDBDatabase *db, const std::string& key, std::string& value, bool& exist);
RunResult fdb_rm_key(FDBDatabase *db, const std::string& key);

typedef std::vector<std::pair<std::string, std::string>> keyvalues;
RunResult fdb_list_key_value(FDBDatabase *db, const std::string& prefix, const std::string& marker, const std::string& delimiter, int max_count, keyvalues& kvs, bool& truncated);
RunResult fdb_list_key_value(FDBDatabase *db, const std::string& startMarker, const std::string& endMarker, int max_count, keyvalues& kvs, bool& truncated);
