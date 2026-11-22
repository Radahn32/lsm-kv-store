#include "kvstore/db_impl.hpp"
#include <gtest/gtest.h>
#include <filesystem>
#include <map>
#include <unistd.h>

namespace kvstore { namespace {
struct DiskMock final:DiskStorage {
 std::vector<InternalKey> written;
 std::map<std::string,std::pair<InternalKey,std::string>> records;
 Status GetAsOf(const std::string& key,Timestamp ts,InternalKey* k,std::string* v)const override{auto i=records.find(key);if(i==records.end()||i->second.first.version>ts)return Status::NotFound();*k=i->second.first;*v=i->second.second;return Status::Ok();}
 Status Scan(const std::string& a,const std::string& b,std::vector<std::pair<std::string,std::string>>* out)const override{for(auto&[k,p]:records)if(k>=a&&(b.empty()||k<b))out->emplace_back(k,p.second);return Status::Ok();}
 Status Flush(Iterator& it)override{for(it.SeekToFirst();it.Valid();it.Next()){auto k=it.Key();written.push_back(k);records[k.user_key]={k,std::string(it.Value())};}return Status::Ok();}
};
struct Fixture {std::string path="/tmp/db-test-"+std::to_string(::getpid())+"-"+std::to_string(++n)+".wal";~Fixture(){std::filesystem::remove(path);}static inline int n=0;
 DBOptions opts(std::shared_ptr<DiskStorage> disk={},std::size_t limit=100000){return {path,limit,std::move(disk)};}};
TEST(DBTest, CRUDTimeTravelDiffAndTombstones){Fixture f;DBImpl db(f.opts());ASSERT_TRUE(db.Open().ok());ASSERT_TRUE(db.Put("k","one").ok());ASSERT_TRUE(db.Put("k","two").ok());std::string v;ASSERT_TRUE(db.GetAsOf("k",1,&v).ok());EXPECT_EQ(v,"one");DiffResult d;ASSERT_TRUE(db.Diff("k",1,2,&d).ok());EXPECT_TRUE(d.changed);EXPECT_EQ(*d.old_value,"one");ASSERT_TRUE(db.Delete("k").ok());EXPECT_TRUE(db.Get("k",&v).is_not_found());EXPECT_TRUE(db.GetAsOf("k",2,&v).ok());}
TEST(DBTest, ReplaysAndContinuesMonotonicVersions){Fixture f;{DBImpl db(f.opts());ASSERT_TRUE(db.Put("k","one").ok());ASSERT_TRUE(db.Put("k","two").ok());}{DBImpl db(f.opts());ASSERT_TRUE(db.Open().ok());std::string v;ASSERT_TRUE(db.GetAsOf("k",1,&v).ok());EXPECT_EQ(v,"one");ASSERT_TRUE(db.Put("k","three").ok());ASSERT_TRUE(db.GetAsOf("k",3,&v).ok());EXPECT_EQ(v,"three");}}
TEST(DBTest, FlushesMemtableAtThresholdAndScanIsSorted){Fixture f;auto disk=std::make_shared<DiskMock>();DBImpl db(f.opts(disk,1));ASSERT_TRUE(db.Put("b","B").ok());ASSERT_TRUE(db.WaitForFlush().ok());ASSERT_FALSE(disk->written.empty());ASSERT_TRUE(db.Put("a","A").ok());std::vector<std::pair<std::string,std::string>> rows;ASSERT_TRUE(db.Scan("a","z",&rows).ok());ASSERT_EQ(rows.size(),2U);EXPECT_EQ(rows[0].first,"a");}
TEST(DBTest, RejectsNullOutputs){Fixture f;DBImpl db(f.opts());EXPECT_EQ(db.Get("x",nullptr).code,Status::Code::kInvalidArgument);DiffResult* p=nullptr;EXPECT_EQ(db.Diff("x",1,2,p).code,Status::Code::kInvalidArgument);}
}}
