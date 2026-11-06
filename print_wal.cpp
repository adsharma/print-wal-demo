// print_wal extension — litestream-style WAL tailer.
//
// Technique (same idea as litestream for SQLite):
//   1. Hold a long-running READ-ONLY transaction open on a dedicated connection.
//      The engine's checkpointer computes a "snapshot horizon" from all open
//      transactions; while our reader is open the checkpoint skips WAL rotation
//      AND retirement (see Checkpointer::postCheckpointCleanup / beginCheckpoint
//      with retentionHorizon != INVALID_TRANSACTION). The WAL file therefore
//      keeps every committed record instead of being truncated/reset.
//   2. A publisher thread tails the WAL file: it polls the file size, opens the
//      WAL fresh, deserializes records with WALRecord::deserialize (honouring
//      the checksum setting), and publishes each new record (prints it).
//      Offsets are tracked so each record is published exactly once; torn
//      tails (partial last write) are ignored until completed.
//
// Usage from the shell / any connection on the same Database:
//      LOAD EXTENSION '/tmp/print_wal/build/libprint_wal.lbug_extension';
//      CALL print_wal_start() RETURN *;    -- pin WAL + start publisher
//      ... run DDL/DML ...
//      CALL print_wal_status() RETURN *;   -- inspect tail offset
//      CALL print_wal_stop() RETURN *;     -- stop publisher, release pin

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "common/file_system/file_info.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/types/types.h"
#include "extension/extension.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_function.h"
#include "function/table/table_function.h"
#include "main/client_context.h"
#include "main/connection.h"
#include "main/database.h"
#include "main/db_config.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_utils.h"
#include "storage/wal/checksum_reader.h"
#include "storage/wal/wal_record.h"
#include "transaction/transaction_context.h"

using namespace lbug;
using namespace lbug::common;
using namespace lbug::function;
using namespace lbug::main;
using namespace lbug::storage;

namespace lbug {
namespace print_wal_extension {

// ---------------------------------------------------------------------------
// Global tailer state (one per process; the demo uses a single Database).
// ---------------------------------------------------------------------------
struct PrintWALState {
    std::mutex mtx;          // guards everything below except stopFlag
    std::mutex printMtx;     // serializes stdout with the rest of the process
    bool running = false;
    std::atomic<bool> stopFlag{false};
    std::thread worker;
    // Dedicated connection holding the long-running read transaction that pins
    // the WAL (prevents checkpoint rotation/retirement). Its ClientContext
    // also serves WALRecord::deserialize on the publisher thread.
    std::unique_ptr<Connection> pinConn;
    Database* db = nullptr;
    std::string dbPath;
    std::string walPath;
    bool enableChecksums = false;
    uint64_t nextOffset = 0; // WAL byte offset of the next unpublished record
    uint64_t published = 0;  // records published so far
};

static PrintWALState& getState() {
    static PrintWALState state;
    return state;
}

// ---------------------------------------------------------------------------
// Record rendering.
// ---------------------------------------------------------------------------
static const char* walRecordTypeName(WALRecordType type) {
    switch (type) {
    case WALRecordType::BEGIN_TRANSACTION_RECORD:
        return "BEGIN_TRANSACTION_RECORD";
    case WALRecordType::COMMIT_RECORD:
        return "COMMIT_RECORD";
    case WALRecordType::ABORT_RECORD:
        return "ABORT_RECORD";
    case WALRecordType::COPY_TABLE_RECORD:
        return "COPY_TABLE_RECORD";
    case WALRecordType::CREATE_CATALOG_ENTRY_RECORD:
        return "CREATE_CATALOG_ENTRY_RECORD";
    case WALRecordType::CREATE_INDEX_RECORD:
        return "CREATE_INDEX_RECORD";
    case WALRecordType::DROP_CATALOG_ENTRY_RECORD:
        return "DROP_CATALOG_ENTRY_RECORD";
    case WALRecordType::ALTER_TABLE_ENTRY_RECORD:
        return "ALTER_TABLE_ENTRY_RECORD";
    case WALRecordType::UPDATE_SEQUENCE_RECORD:
        return "UPDATE_SEQUENCE_RECORD";
    case WALRecordType::UPDATE_SEQUENCE_NAMED_RECORD:
        return "UPDATE_SEQUENCE_NAMED_RECORD";
    case WALRecordType::TABLE_INSERTION_RECORD:
        return "TABLE_INSERTION_RECORD";
    case WALRecordType::NODE_DELETION_RECORD:
        return "NODE_DELETION_RECORD";
    case WALRecordType::NODE_UPDATE_RECORD:
        return "NODE_UPDATE_RECORD";
    case WALRecordType::REL_DELETION_RECORD:
        return "REL_DELETION_RECORD";
    case WALRecordType::REL_DETACH_DELETE_RECORD:
        return "REL_DETACH_DELETE_RECORD";
    case WALRecordType::REL_UPDATE_RECORD:
        return "REL_UPDATE_RECORD";
    case WALRecordType::LOAD_EXTENSION_RECORD:
        return "LOAD_EXTENSION_RECORD";
    case WALRecordType::CHECKPOINT_RECORD:
        return "CHECKPOINT_RECORD";
    default:
        return "UNKNOWN_RECORD";
    }
}

// NOTE: deliberately no downcast/dynamic_cast to record subclasses here. The
// concrete record classes have hidden RTTI across the extension/host DSO
// boundary, so cross-module dynamic_cast fails at load time. Re-serializing
// is also off the table: deserialize->serialize round-trips are not supported
// for catalog-entry records. The published detail is therefore the record's
// framing (byte range in the WAL) plus its owner catalog.

static void publishRecord(uint64_t offset, uint64_t endOffset, const WALRecord& record) {
    auto& s = getState();
    std::lock_guard<std::mutex> lck{s.printMtx};
    std::printf("[print_wal] offset=%llu bytes=%llu type=%s owner='%s'\n",
        (unsigned long long)offset, (unsigned long long)(endOffset - offset),
        walRecordTypeName(record.type), record.ownerCatalogName.c_str());
    std::fflush(stdout);
}

// ---------------------------------------------------------------------------
// Publisher thread: tails the WAL file and publishes new records.
// ---------------------------------------------------------------------------
static Deserializer makeDeserializer(FileInfo& fileInfo, bool enableChecksums,
    ClientContext* context) {
    if (enableChecksums) {
        return Deserializer{std::make_unique<ChecksumReader>(fileInfo,
            *MemoryManager::Get(*context), "print_wal: WAL checksum mismatch")};
    }
    return Deserializer{std::make_unique<BufferedFileReader>(fileInfo)};
}

static void publisherLoop(Database* db, ClientContext* pinCtx, std::string walPath,
    bool enableChecksums) {
    auto& s = getState();
    uint64_t lastSize = 0;
    while (!s.stopFlag.load(std::memory_order_acquire)) {
        try {
            // Fresh stat each poll; the pinned WAL is append-only so offsets
            // stay valid (a shrink means rotation happened -> restart at 0).
            uint64_t size = 0;
            bool exists = false;
            try {
                exists = std::filesystem::exists(walPath);
                if (exists) {
                    size = std::filesystem::file_size(walPath);
                }
            } catch (...) {
                exists = false;
            }
            if (!exists) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            uint64_t next = 0;
            {
                std::lock_guard<std::mutex> lck{s.mtx};
                next = s.nextOffset;
            }
            if (size < next) {
                // WAL was rotated/retired (should not happen while pinned).
                std::lock_guard<std::mutex> lck{s.mtx};
                s.nextOffset = 0;
                next = 0;
            }
            if (size != lastSize || next == 0) {
                auto* vfs = db->getVFS();
                auto fileInfo = vfs->openFile(walPath, FileOpenFlags(FileFlags::READ_ONLY));
                if (fileInfo->getFileSize() == 0) {
                    lastSize = size;
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    continue;
                }
                Deserializer de = makeDeserializer(*fileInfo, enableChecksums, pinCtx);
                // WAL header (read exactly like recovery does: as the first
                // checksummed/plain object, so offsets stay consistent).
                de.getReader()->onObjectBegin();
                uuid walDatabaseID{};
                de.deserializeValue(walDatabaseID);
                uint8_t checksumFlag = 0;
                de.deserializeValue(checksumFlag);
                de.getReader()->onObjectEnd();
                const uint64_t firstRecordOffset = de.getReadOffset();
                if (next == 0) {
                    std::lock_guard<std::mutex> lck{s.mtx};
                    if (s.nextOffset == 0) {
                        s.nextOffset = firstRecordOffset;
                    }
                    next = s.nextOffset;
                }
                // Rescan from the start (cheap for a demo), publishing only
                // records at/after `next`. A deserialization failure means a
                // torn tail — stop and retry on the next poll.
                while (!de.finished()) {
                    if (s.stopFlag.load(std::memory_order_acquire)) {
                        break;
                    }
                    const uint64_t recordStart = de.getReadOffset();
                    std::unique_ptr<WALRecord> record;
                    try {
                        record = WALRecord::deserialize(de, *pinCtx);
                    } catch (...) {
                        break; // torn tail; the next poll retries
                    }
                    const uint64_t recordEnd = de.getReadOffset();
                    if (recordStart >= next) {
                        publishRecord(recordStart, recordEnd, *record);
                        std::lock_guard<std::mutex> lck{s.mtx};
                        s.published++;
                        s.nextOffset = recordEnd;
                        next = recordEnd;
                    }
                }
                lastSize = size;
            }
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lck{s.printMtx};
            std::printf("[print_wal] tail error (will retry): %s\n", e.what());
            std::fflush(stdout);
        } catch (...) {
            std::lock_guard<std::mutex> lck{s.printMtx};
            std::printf("[print_wal] tail error (will retry): unknown\n");
            std::fflush(stdout);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

// ---------------------------------------------------------------------------
// Core start/stop/status operations (called from the table-function binds).
// ---------------------------------------------------------------------------
static uint64_t currentWALFileSize(const std::string& walPath) {
    try {
        if (std::filesystem::exists(walPath)) {
            return std::filesystem::file_size(walPath);
        }
    } catch (...) {}
    return 0;
}

// Begins the pinning read transaction and spawns the publisher. Idempotent.
static void startTailing(const ClientContext* context) {
    auto& s = getState();
    std::lock_guard<std::mutex> lck{s.mtx};
    if (s.running) {
        return;
    }
    auto* db = context->getDatabase();
    const auto dbPath = context->getDatabasePath();
    if (dbPath.empty() || dbPath == ":memory:") {
        throw RuntimeException("print_wal requires an on-disk database (no WAL for :memory:).");
    }
    s.db = db;
    s.dbPath = dbPath;
    s.walPath = StorageUtils::getWALFilePath(dbPath);
    s.enableChecksums = context->getDBConfig()->enableChecksums;
    // Dedicated connection holding ONE long-running read-only transaction.
    // Its start timestamp pins the checkpoint snapshot horizon, so checkpoints
    // skip WAL rotation/retirement while it is open (litestream technique).
    s.pinConn = std::make_unique<Connection>(db);
    transaction::TransactionContext::Get(*s.pinConn->getClientContext())->beginReadTransaction();
    s.nextOffset = 0;
    // Best-effort synchronous init of nextOffset past the WAL header so the
    // START row reports a meaningful offset even before the first poll.
    try {
        if (std::filesystem::exists(s.walPath) &&
            std::filesystem::file_size(s.walPath) > 0) {
            auto fileInfo =
                s.db->getVFS()->openFile(s.walPath, FileOpenFlags(FileFlags::READ_ONLY));
            Deserializer de =
                makeDeserializer(*fileInfo, s.enableChecksums, s.pinConn->getClientContext());
            de.getReader()->onObjectBegin();
            uuid walDatabaseID{};
            de.deserializeValue(walDatabaseID);
            uint8_t checksumFlag = 0;
            de.deserializeValue(checksumFlag);
            de.getReader()->onObjectEnd();
            s.nextOffset = de.getReadOffset();
        }
    } catch (...) {}
    s.published = 0;
    s.stopFlag.store(false, std::memory_order_release);
    s.running = true;
    // Snapshot raw pointers as thread args: the pin Connection object stays alive
    // until after the worker is joined (stopTailing moves it out and destroys it
    // post-join), and the Database outlives the tailing session (STOP must run
    // before Database destruction). The worker never touches the movable state.
    // Spawning while holding mtx is safe: we never join while holding it.
    auto* dbPtr = s.db;
    auto* pinCtx = s.pinConn->getClientContext();
    s.worker = std::thread([dbPtr, pinCtx, walPath = s.walPath, checksums = s.enableChecksums] {
        publisherLoop(dbPtr, pinCtx, walPath, checksums);
    });
}

static void stopTailing() {
    auto& s = getState();
    std::thread worker;
    std::unique_ptr<Connection> pinConn;
    {
        std::lock_guard<std::mutex> lck{s.mtx};
        if (!s.running) {
            return;
        }
        s.stopFlag.store(true, std::memory_order_release);
        // Move the handles out under the lock so concurrent stops cannot
        // double-join; the worker observes stopFlag and exits on its own.
        worker = std::move(s.worker);
        pinConn = std::move(s.pinConn);
        s.running = false;
        s.db = nullptr;
    }
    if (worker.joinable()) {
        worker.join();
    }
    // Rolling back the pin transaction unpins the horizon; the next checkpoint
    // may rotate/retire the WAL again.
    if (pinConn) {
        try {
            transaction::TransactionContext::Get(*pinConn->getClientContext())->rollback();
        } catch (...) {}
    }
}

// ---------------------------------------------------------------------------
// CALL print_wal_start() RETURN *  -> (msg, wal_path, start_offset)
// ---------------------------------------------------------------------------
struct PrintWALStartBindData final : TableFuncBindData {
    std::string msg;
    std::string walPath;
    uint64_t startOffset = 0;

    PrintWALStartBindData(std::string msg, std::string walPath, uint64_t startOffset,
        binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), 1}, msg{std::move(msg)},
          walPath{std::move(walPath)}, startOffset{startOffset} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<PrintWALStartBindData>(*this);
    }
};

struct PrintWALStartFunction {
    static constexpr const char* name = "PRINT_WAL_START";

    static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext* context,
        const TableFuncBindInput* input) {
        startTailing(context);
        auto& s = getState();
        std::lock_guard<std::mutex> lck{s.mtx};
        std::vector<std::string> names = {"msg", "wal_path", "start_offset"};
        std::vector<LogicalType> types;
        types.emplace_back(LogicalType::STRING());
        types.emplace_back(LogicalType::STRING());
        types.emplace_back(LogicalType::UINT64());
        names = TableFunction::extractYieldVariables(names, input->yieldVariables);
        return std::make_unique<PrintWALStartBindData>("tailing started", s.walPath, s.nextOffset,
            input->binder->createVariables(names, types));
    }

    static offset_t internalTableFunc(const TableFuncMorsel& morsel, const TableFuncInput& input,
        DataChunk& output) {
        const auto* bindData = input.bindData->constPtrCast<PrintWALStartBindData>();
        const auto numTuples = morsel.getMorselSize();
        for (auto i = 0u; i < numTuples; i++) {
            output.getValueVectorMutable(0).setValue(i, bindData->msg);
            output.getValueVectorMutable(1).setValue(i, bindData->walPath);
            output.getValueVectorMutable(2).setValue(i, bindData->startOffset);
        }
        return numTuples;
    }

    static function_set getFunctionSet() {
        function_set set;
        auto fn = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
        fn->bindFunc = bindFunc;
        fn->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
        fn->initSharedStateFunc = SimpleTableFunc::initSharedState;
        fn->initLocalStateFunc = TableFunction::initEmptyLocalState;
        set.push_back(std::move(fn));
        return set;
    }
};

// ---------------------------------------------------------------------------
// CALL print_wal_status() RETURN * -> (running, wal_path, next_offset,
//                                      wal_size, published)
// ---------------------------------------------------------------------------
struct PrintWALStatusBindData final : TableFuncBindData {
    bool running = false;
    std::string walPath;
    uint64_t nextOffset = 0;
    uint64_t walSize = 0;
    uint64_t published = 0;

    PrintWALStatusBindData(bool running, std::string walPath, uint64_t nextOffset,
        uint64_t walSize, uint64_t published, binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), 1}, running{running}, walPath{std::move(walPath)},
          nextOffset{nextOffset}, walSize{walSize}, published{published} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<PrintWALStatusBindData>(*this);
    }
};

struct PrintWALStatusFunction {
    static constexpr const char* name = "PRINT_WAL_STATUS";

    static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext*,
        const TableFuncBindInput* input) {
        auto& s = getState();
        std::lock_guard<std::mutex> lck{s.mtx};
        std::vector<std::string> names = {"running", "wal_path", "next_offset", "wal_size",
            "published"};
        std::vector<LogicalType> types;
        types.emplace_back(LogicalType::BOOL());
        types.emplace_back(LogicalType::STRING());
        types.emplace_back(LogicalType::UINT64());
        types.emplace_back(LogicalType::UINT64());
        types.emplace_back(LogicalType::UINT64());
        names = TableFunction::extractYieldVariables(names, input->yieldVariables);
        const auto walSize = currentWALFileSize(s.walPath);
        return std::make_unique<PrintWALStatusBindData>(s.running, s.walPath, s.nextOffset,
            walSize, s.published, input->binder->createVariables(names, types));
    }

    static offset_t internalTableFunc(const TableFuncMorsel& morsel, const TableFuncInput& input,
        DataChunk& output) {
        const auto* bindData = input.bindData->constPtrCast<PrintWALStatusBindData>();
        const auto numTuples = morsel.getMorselSize();
        for (auto i = 0u; i < numTuples; i++) {
            output.getValueVectorMutable(0).setValue(i, bindData->running);
            output.getValueVectorMutable(1).setValue(i, bindData->walPath);
            output.getValueVectorMutable(2).setValue(i, bindData->nextOffset);
            output.getValueVectorMutable(3).setValue(i, bindData->walSize);
            output.getValueVectorMutable(4).setValue(i, bindData->published);
        }
        return numTuples;
    }

    static function_set getFunctionSet() {
        function_set set;
        auto fn = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
        fn->bindFunc = bindFunc;
        fn->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
        fn->initSharedStateFunc = SimpleTableFunc::initSharedState;
        fn->initLocalStateFunc = TableFunction::initEmptyLocalState;
        set.push_back(std::move(fn));
        return set;
    }
};

// ---------------------------------------------------------------------------
// CALL print_wal_stop() RETURN * -> (msg, published)
// ---------------------------------------------------------------------------
struct PrintWALStopBindData final : TableFuncBindData {
    std::string msg;
    uint64_t published = 0;

    PrintWALStopBindData(std::string msg, uint64_t published,
        binder::expression_vector columns)
        : TableFuncBindData{std::move(columns), 1}, msg{std::move(msg)}, published{published} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<PrintWALStopBindData>(*this);
    }
};

struct PrintWALStopFunction {
    static constexpr const char* name = "PRINT_WAL_STOP";

    static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext*,
        const TableFuncBindInput* input) {
        auto& s = getState();
        bool wasRunning = false;
        {
            std::lock_guard<std::mutex> lck{s.mtx};
            wasRunning = s.running;
        }
        uint64_t published = 0;
        std::string msg;
        if (wasRunning) {
            stopTailing();
            std::lock_guard<std::mutex> lck{s.mtx};
            published = s.published;
            msg = "tailing stopped";
        } else {
            std::lock_guard<std::mutex> lck{s.mtx};
            published = s.published;
            msg = "not running";
        }
        std::vector<std::string> names = {"msg", "published"};
        std::vector<LogicalType> types;
        types.emplace_back(LogicalType::STRING());
        types.emplace_back(LogicalType::UINT64());
        names = TableFunction::extractYieldVariables(names, input->yieldVariables);
        return std::make_unique<PrintWALStopBindData>(msg, published,
            input->binder->createVariables(names, types));
    }

    static offset_t internalTableFunc(const TableFuncMorsel& morsel, const TableFuncInput& input,
        DataChunk& output) {
        const auto* bindData = input.bindData->constPtrCast<PrintWALStopBindData>();
        const auto numTuples = morsel.getMorselSize();
        for (auto i = 0u; i < numTuples; i++) {
            output.getValueVectorMutable(0).setValue(i, bindData->msg);
            output.getValueVectorMutable(1).setValue(i, bindData->published);
        }
        return numTuples;
    }

    static function_set getFunctionSet() {
        function_set set;
        auto fn = std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
        fn->bindFunc = bindFunc;
        fn->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
        fn->initSharedStateFunc = SimpleTableFunc::initSharedState;
        fn->initLocalStateFunc = TableFunction::initEmptyLocalState;
        set.push_back(std::move(fn));
        return set;
    }
};

class PrintWALExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "PRINT_WAL";

    static void load(ClientContext* context) {
        auto& db = *context->getDatabase();
        extension::ExtensionUtils::addStandaloneTableFunc<PrintWALStartFunction>(db);
        extension::ExtensionUtils::addStandaloneTableFunc<PrintWALStatusFunction>(db);
        extension::ExtensionUtils::addStandaloneTableFunc<PrintWALStopFunction>(db);
    }
};

} // namespace print_wal_extension
} // namespace lbug

#if defined(BUILD_DYNAMIC_LOAD)
extern "C" {
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(lbug::main::ClientContext* context) {
    lbug::print_wal_extension::PrintWALExtension::load(context);
}

INIT_EXPORT const char* name() {
    return lbug::print_wal_extension::PrintWALExtension::EXTENSION_NAME;
}
}
#endif
