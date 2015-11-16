/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include <tellstore/ClientConfig.hpp>
#include <tellstore/ClientManager.hpp>
#include <tellstore/GenericTuple.hpp>
#include <tellstore/Record.hpp>
#include <tellstore/ScanMemory.hpp>
#include <tellstore/TransactionRunner.hpp>

#include <crossbow/byte_buffer.hpp>
#include <crossbow/enum_underlying.hpp>
#include <crossbow/infinio/InfinibandService.hpp>
#include <crossbow/logger.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/string.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <system_error>

using namespace tell::store;

namespace {

class OperationTimer {
public:
    OperationTimer()
            : mTotalDuration(0x0u) {
    }

    void start() {
        mStartTime = std::chrono::steady_clock::now();
    }

    std::chrono::nanoseconds stop() {
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()
                - mStartTime);
        mTotalDuration += duration;
        return duration;
    }

    std::chrono::nanoseconds total() const {
        return mTotalDuration;
    }

private:
    std::chrono::steady_clock::time_point mStartTime;
    std::chrono::nanoseconds mTotalDuration;
};

class TestClient {
public:
    TestClient(const ClientConfig& config, size_t scanMemoryLength, size_t numTuple, size_t numTransactions);

    void run();

    void shutdown();

private:
    void addTable(ClientHandle& client);

    void executeTransaction(ClientHandle& client, uint64_t startKey, uint64_t endKey);

    //void executeScan(ClientHandle& handle, float selectivity);

    //void executeProjection(ClientHandle& client, float selectivity);

    void executeAggregation(ClientHandle& client, float selectivity);

    ClientManager<void> mManager;

    std::unique_ptr<ScanMemoryManager> mScanMemory;

    /// Number of tuples to insert per transaction
    size_t mNumTuple;

    /// Number of concurrent transactions to start
    size_t mNumTransactions;

    Table mTable;
};

TestClient::TestClient(const ClientConfig& config, size_t scanMemoryLength, size_t numTuple, size_t numTransactions)
        : mManager(config),
          mScanMemory(mManager.allocateScanMemory(config.tellStore.size(), scanMemoryLength / config.tellStore.size())),
          mNumTuple(numTuple),
          mNumTransactions(numTransactions) {
    LOG_INFO("Initialized TellStore client");
}

void TestClient::run() {
    LOG_INFO("Starting test workload");
    auto startTime = std::chrono::steady_clock::now();

    LOG_INFO("Start create table transaction");
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::addTable, this, std::placeholders::_1));

    LOG_INFO("Starting %1% test load transaction(s)", mNumTransactions);
    MultiTransactionRunner<void> runner(mManager);
    for (decltype(mNumTransactions) i = 0; i < mNumTransactions; ++i) {
        auto startRange = i * mNumTuple;
        auto endRange = startRange + mNumTuple;
        runner.execute(std::bind(&TestClient::executeTransaction, this, std::placeholders::_1, startRange, endRange));
    }
    runner.wait();

    LOG_INFO("Starting test scan transaction(s)");
    //TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeScan, this, std::placeholders::_1, 1.0));
    //TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeScan, this, std::placeholders::_1, 0.5));
    //TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeScan, this, std::placeholders::_1, 0.25));
    //TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeProjection, this, std::placeholders::_1, 1.0));
    //TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeProjection, this, std::placeholders::_1, 0.5));
    //TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeProjection, this, std::placeholders::_1, 0.25));
    //TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeAggregation, this, std::placeholders::_1, 1.0));
    //TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeAggregation, this, std::placeholders::_1, 0.5));
    TransactionRunner::executeBlocking(mManager, std::bind(&TestClient::executeAggregation, this, std::placeholders::_1, 0.25));

    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::duration<double>>(endTime - startTime);
    LOG_INFO("Running test workload took %1%s", duration.count());
}

void TestClient::shutdown() {
    LOG_INFO("Shutting down the TellStore client");

    mManager.shutdown();
}

void TestClient::addTable(ClientHandle& client) {
    LOG_TRACE("Adding table");
    Schema schema(TableType::TRANSACTIONAL);
    schema.addField(FieldType::FLOAT, "salary", true);
    schema.addField(FieldType::TEXT, "text1", true);
    schema.addField(FieldType::TEXT, "text2", true);
    schema.addField(FieldType::TEXT, "text3", true);

    auto startTime = std::chrono::steady_clock::now();
    mTable = client.createTable("testTable", std::move(schema));
    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
    LOG_INFO("Adding table took %1%ns", duration.count());
}

template<class RandomDevice>
crossbow::string astring(RandomDevice& randomDevice, int x, int y) {
    std::uniform_int_distribution<int> lenDist(x, y);
    std::uniform_int_distribution<int> charDist(0, 255);
    auto length = lenDist(randomDevice);
    crossbow::string result;
    result.reserve(length);
    for (decltype(length) i = 0; i < length; ++i) {
        int charPos = charDist(randomDevice);
        if (charPos < 95) {
            result.push_back(char(0x21 + charPos)); // a printable ASCII/UTF-8 character
            continue;
        }
        constexpr uint16_t lowest6 = 0x3f;
        uint16_t unicodeValue = 0xc0 + (charPos - 95); // a printable unicode character
        // convert this to UTF-8
        uint8_t utf8[2] = {0xc0, 0x80}; // The UTF-8 base for 2-byte Characters
        // for the first char, we have to take the lowest 6 bit
        utf8[1] |= uint8_t(unicodeValue & lowest6);
        utf8[0] |= uint8_t(unicodeValue >> 6); // get the remaining bits
        assert((utf8[0] >> 5) == uint8_t(0x06)); // higher order byte starts with 110
        assert((utf8[1] >> 6) == uint8_t(0x2)); // lower order byte starts with 10
        result.push_back(*reinterpret_cast<char*>(utf8));
        result.push_back(*reinterpret_cast<char*>(utf8 + 1));
    }
    return result;
}

void TestClient::executeTransaction(ClientHandle& client, uint64_t startKey, uint64_t endKey) {
    LOG_TRACE("Starting transaction");
    auto snapshot = client.startTransaction();
    LOG_INFO("TID %1%] Started transaction", snapshot->version());

    crossbow::string str("Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, conset");
    OperationTimer insertTimer;
    OperationTimer getTimer;
    auto startTime = std::chrono::steady_clock::now();
    std::random_device realRandom;
    std::mt19937 randDevice(realRandom());
    std::deque<std::shared_ptr<ModificationResponse>> futures;
    std::uniform_real_distribution<float> dist(0.0, 1.0);
    for (auto key = startKey; key < endKey; ++key) {
        futures.emplace_back(client.insert(mTable, key, *snapshot, GenericTuple{
            {"salary", dist(randDevice)},
            {"text1", str},
            {"text2", str},
            {"text3", str}
        }));
        if (futures.size() == 50) {
            if (!futures.front()->waitForResult()) {
                std::cerr << "";
                std::terminate();
            }
            futures.front()->waitForResult();
            futures.pop_front();
        }
        if (key % 100000 == 0) {
            std::cout << "inserted " << key - startKey << std::endl;
        }
    }
    while (!futures.empty()) {
        if (!futures.front()->waitForResult()) {
            std::cerr << "";
            std::terminate();
        }
        futures.pop_front();
    }

    LOG_TRACE("Commit transaction");
    client.commit(*snapshot);

    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    LOG_INFO("TID %1%] Transaction completed in %2%ms [total = %3%ms / %4%ms, average = %5%us / %6%us]",
             snapshot->version(),
             duration.count(),
             std::chrono::duration_cast<std::chrono::milliseconds>(insertTimer.total()).count(),
             std::chrono::duration_cast<std::chrono::milliseconds>(getTimer.total()).count(),
             std::chrono::duration_cast<std::chrono::microseconds>(insertTimer.total()).count() / (endKey - startKey),
             std::chrono::duration_cast<std::chrono::microseconds>(getTimer.total()).count() / (endKey - startKey));
}

//void TestClient::executeScan(ClientHandle& client, float selectivity) {
//    LOG_TRACE("Starting transaction");
//    auto snapshot = client.startTransaction(TransactionType::READ_ONLY);
//    LOG_INFO("TID %1%] Starting full scan with selectivity %2%%%", snapshot->version(),
//            static_cast<int>(selectivity * 100));
//
//    Record::id_t recordField;
//    if (!mTable.record().idOf("number", recordField)) {
//        LOG_ERROR("number field not found");
//        return;
//    }
//
//    uint32_t selectionLength = 24;
//    std::unique_ptr<char[]> selection(new char[selectionLength]);
//
//    crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
//    selectionWriter.write<uint64_t>(0x1u);
//    selectionWriter.write<uint16_t>(recordField);
//    selectionWriter.write<uint16_t>(0x1u);
//    selectionWriter.align(sizeof(uint64_t));
//    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER_EQUAL));
//    selectionWriter.write<uint8_t>(0x0u);
//    selectionWriter.align(sizeof(uint32_t));
//    selectionWriter.write<int32_t>(mTuple.size() - mTuple.size() * selectivity);
//
//    auto scanStartTime = std::chrono::steady_clock::now();
//    auto scanIterator = client.scan(mTable, *snapshot, *mScanMemory, ScanQueryType::FULL, selectionLength,
//            selection.get(), 0x0u, nullptr);
//
//    size_t scanCount = 0x0u;
//    size_t scanDataSize = 0x0u;
//    while (scanIterator->hasNext()) {
//        uint64_t key;
//        const char* tuple;
//        size_t tupleLength;
//        std::tie(key, tuple, tupleLength) = scanIterator->next();
//        ++scanCount;
//        scanDataSize += tupleLength;
//    }
//    auto scanEndTime = std::chrono::steady_clock::now();
//
//    if (scanIterator->error()) {
//        auto& ec = scanIterator->error();
//        LOG_ERROR("Error scanning table [error = %1% %2%]", ec, ec.message());
//        return;
//    }
//
//    LOG_TRACE("Commit transaction");
//    client.commit(*snapshot);
//
//    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
//    auto scanTotalDataSize = double(scanDataSize) / double(1024 * 1024 * 1024);
//    auto scanBandwidth = double(scanDataSize * 8) / double(1000 * 1000 * 1000 *
//            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
//    auto scanTupleSize = (scanCount == 0u ? 0u : scanDataSize / scanCount);
//    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of average size %4% (%5%GiB total, %6%Gbps bandwidth)]",
//            snapshot->version(), scanDuration.count(), scanCount, scanTupleSize, scanTotalDataSize, scanBandwidth);
//}

//void TestClient::executeProjection(ClientHandle& client, float selectivity) {
//    LOG_TRACE("Starting transaction");
//    auto snapshot = client.startTransaction(TransactionType::READ_ONLY);
//    LOG_INFO("TID %1%] Starting projection scan with selectivity %2%%%", snapshot->version(),
//            static_cast<int>(selectivity * 100));
//
//    Record::id_t numberField;
//    if (!mTable.record().idOf("number", numberField)) {
//        LOG_ERROR("number field not found");
//        return;
//    }
//
//    Record::id_t text2Field;
//    if (!mTable.record().idOf("text2", text2Field)) {
//        LOG_ERROR("text2 field not found");
//        return;
//    }
//
//    uint32_t selectionLength = 24;
//    std::unique_ptr<char[]> selection(new char[selectionLength]);
//
//    crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
//    selectionWriter.write<uint64_t>(0x1u);
//    selectionWriter.write<uint16_t>(numberField);
//    selectionWriter.write<uint16_t>(0x1u);
//    selectionWriter.align(sizeof(uint64_t));
//    selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::GREATER_EQUAL));
//    selectionWriter.write<uint8_t>(0x0u);
//    selectionWriter.align(sizeof(uint32_t));
//    selectionWriter.write<int32_t>(mTuple.size() - mTuple.size() * selectivity);
//
//    uint32_t projectionLength = 4;
//    std::unique_ptr<char[]> projection(new char[projectionLength]);
//
//    crossbow::buffer_writer projectionWriter(projection.get(), projectionLength);
//    projectionWriter.write<uint16_t>(numberField);
//    projectionWriter.write<uint16_t>(text2Field);
//
//    Schema resultSchema(mTable.tableType());
//    resultSchema.addField(FieldType::INT, "number", true);
//    resultSchema.addField(FieldType::TEXT, "text2", true);
//    Table resultTable(mTable.tableId(), std::move(resultSchema));
//
//    auto scanStartTime = std::chrono::steady_clock::now();
//    auto scanIterator = client.scan(resultTable, *snapshot, *mScanMemory, ScanQueryType::PROJECTION, selectionLength,
//            selection.get(), projectionLength, projection.get());
//
//    size_t scanCount = 0x0u;
//    size_t scanDataSize = 0x0u;
//    while (scanIterator->hasNext()) {
//        uint64_t key;
//        const char* tuple;
//        size_t tupleLength;
//        std::tie(key, tuple, tupleLength) = scanIterator->next();
//        ++scanCount;
//        scanDataSize += tupleLength;
//
//    }
//
//    auto scanEndTime = std::chrono::steady_clock::now();
//    if (scanIterator->error()) {
//        auto& ec = scanIterator->error();
//        LOG_ERROR("Error scanning table [error = %1% %2%]", ec, ec.message());
//        return;
//    }
//
//    LOG_TRACE("Commit transaction");
//    client.commit(*snapshot);
//
//    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
//    auto scanTotalDataSize = double(scanDataSize) / double(1024 * 1024 * 1024);
//    auto scanBandwidth = double(scanDataSize * 8) / double(1000 * 1000 * 1000 *
//            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
//    auto scanTupleSize = (scanCount == 0u ? 0u : scanDataSize / scanCount);
//    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of average size %4% (%5%GiB total, %6%Gbps bandwidth)]",
//            snapshot->version(), scanDuration.count(), scanCount, scanTupleSize, scanTotalDataSize, scanBandwidth);
//}

void TestClient::executeAggregation(ClientHandle& client, float selectivity) {
    LOG_TRACE("Starting transaction");
    //auto& fiber = client.fiber();
    auto snapshot = client.startTransaction(TransactionType::READ_ONLY);
    LOG_INFO("TID %1%] Starting aggregation scan with selectivity %2%%%", snapshot->version(),
            static_cast<int>(selectivity * 100));

    Record::id_t recordField;
    if (!mTable.record().idOf("salary", recordField)) {
        LOG_ERROR("number field not found");
        return;
    }

    //uint32_t selectionLength = 24;
    uint32_t selectionLength = 8;
    std::unique_ptr<char[]> selection(new char[selectionLength]);

    crossbow::buffer_writer selectionWriter(selection.get(), selectionLength);
    selectionWriter.write<uint64_t>(0x0u);
    //selectionWriter.write<uint16_t>(recordField);
    //selectionWriter.write<uint16_t>(0x1u);
    //selectionWriter.align(sizeof(uint64_t));
    //selectionWriter.write<uint8_t>(crossbow::to_underlying(PredicateType::LESS_EQUAL));
    //selectionWriter.write<uint8_t>(0x0u);
    //selectionWriter.align(sizeof(uint32_t));
    //selectionWriter.write<float>(selectivity);

    uint32_t aggregationLength = 4;
    std::unique_ptr<char[]> aggregation(new char[aggregationLength]);

    crossbow::buffer_writer aggregationWriter(aggregation.get(), aggregationLength);
    aggregationWriter.write<uint16_t>(recordField);
    aggregationWriter.write<uint16_t>(crossbow::to_underlying(AggregationType::SUM));

    Schema resultSchema(mTable.tableType());
    resultSchema.addField(FieldType::FLOAT, "sum", true);
    Table resultTable(mTable.tableId(), std::move(resultSchema));

    auto scanStartTime = std::chrono::steady_clock::now();
    auto scanIterator = client.scan(resultTable, *snapshot, *mScanMemory, ScanQueryType::AGGREGATION, selectionLength,
            selection.get(), aggregationLength, aggregation.get());

    size_t scanCount = 0x0u;
    size_t scanDataSize = 0x0u;
    float totalSum = 0;
    while (scanIterator->hasNext()) {
        const char* tuple;
        size_t tupleLength;
        std::tie(std::ignore, tuple, tupleLength) = scanIterator->next();
        ++scanCount;
        scanDataSize += tupleLength;

        totalSum += resultTable.field<float>("sum", tuple);

        if (scanCount % 1000 == 0) {
            //fiber.yield();
        }
    }

    auto scanEndTime = std::chrono::steady_clock::now();
    if (scanIterator->error()) {
        auto& ec = scanIterator->error();
        LOG_ERROR("Error scanning table [error = %1% %2%]", ec, ec.message());
        return;
    }

    LOG_INFO("TID %1%] Scan output [sum = %2%]", snapshot->version(), totalSum);

    LOG_TRACE("Commit transaction");
    client.commit(*snapshot);

    auto scanDuration = std::chrono::duration_cast<std::chrono::milliseconds>(scanEndTime - scanStartTime);
    auto scanTotalDataSize = double(scanDataSize) / double(1024 * 1024 * 1024);
    auto scanBandwidth = double(scanDataSize * 8) / double(1000 * 1000 * 1000 *
            std::chrono::duration_cast<std::chrono::duration<float>>(scanEndTime - scanStartTime).count());
    auto scanTupleSize = (scanCount == 0u ? 0u : scanDataSize / scanCount);
    LOG_INFO("TID %1%] Scan took %2%ms [%3% tuples of average size %4% (%5%GiB total, %6%Gbps bandwidth)]",
            snapshot->version(), scanDuration.count(), scanCount, scanTupleSize, scanTotalDataSize, scanBandwidth);
}

} // anonymous namespace

int main(int argc, const char** argv) {
    crossbow::string commitManagerHost;
    crossbow::string tellStoreHost;
    size_t scanMemoryLength = 0x80000000ull;
    size_t numTuple = 1000000ull;
    size_t numTransactions = 10;
    tell::store::ClientConfig clientConfig;
    bool help = false;
    crossbow::string logLevel("DEBUG");

    auto opts = crossbow::program_options::create_options(argv[0],
            crossbow::program_options::value<'h'>("help", &help),
            crossbow::program_options::value<'l'>("log-level", &logLevel),
            crossbow::program_options::value<'c'>("commit-manager", &commitManagerHost),
            crossbow::program_options::value<'s'>("server", &tellStoreHost),
            crossbow::program_options::value<'m'>("memory", &scanMemoryLength),
            crossbow::program_options::value<'n'>("tuple", &numTuple),
            crossbow::program_options::value<-1>("network-threads", &clientConfig.numNetworkThreads,
                crossbow::program_options::tag::ignore_short<true>{}));

    try {
        crossbow::program_options::parse(opts, argc, argv);
    } catch (crossbow::program_options::argument_not_found e) {
        std::cerr << e.what() << std::endl << std::endl;
        crossbow::program_options::print_help(std::cout, opts);
        return 1;
    }

    if (help) {
        crossbow::program_options::print_help(std::cout, opts);
        return 0;
    }

    clientConfig.commitManager = ClientConfig::parseCommitManager(commitManagerHost);
    clientConfig.tellStore = ClientConfig::parseTellStore(tellStoreHost);

    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);

    LOG_INFO("Starting TellStore test client");
    LOG_INFO("--- Commit Manager: %1%", clientConfig.commitManager);
    for (auto& ep : clientConfig.tellStore) {
        LOG_INFO("--- TellStore Shards: %1%", ep);
    }
    LOG_INFO("--- Network Threads: %1%", clientConfig.numNetworkThreads);
    LOG_INFO("--- Scan Memory: %1%GB", double(scanMemoryLength) / double(1024 * 1024 * 1024));
    LOG_INFO("--- Number of tuples: %1%", numTuple);
    LOG_INFO("--- Number of transactions: %1%", numTransactions);

    // Initialize network stack
    TestClient client(clientConfig, scanMemoryLength, numTuple, numTransactions);
    client.run();
    client.shutdown();

    LOG_INFO("Exiting TellStore client");
    return 0;
}
