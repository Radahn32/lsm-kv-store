#include "kvstore/db_impl.hpp"

#include <algorithm>
#include <limits>
#include <map>

namespace kvstore {
DBImpl::DBImpl(DBOptions options)
    : options_(std::move(options)), wal_(options_.wal_path), active_(std::make_shared<MemTable>()) {}
DBImpl::~DBImpl() {
    { std::lock_guard lock(flush_mutex_); stopping_=true; }
    flush_cv_.notify_all();
    if (flush_thread_.joinable()) flush_thread_.join();
}
Status DBImpl::Open() {
    std::lock_guard writer(writer_mutex_);
    if (opened_) return Status::Ok();
    auto status=wal_.Replay(active_.get()); if(!status.ok()) return status;
    auto it=active_->NewIterator(); Timestamp largest=0;
    for(it->SeekToFirst();it->Valid();it->Next()) largest=std::max(largest,it->Key().version);
    if(largest==std::numeric_limits<Timestamp>::max()) return Status::Corruption("WAL timestamp exhausted");
    next_version_=largest+1;
    opened_=true;
    flush_thread_=std::thread(&DBImpl::FlushWorker,this);
    return Status::Ok();
}
Status DBImpl::Write(const std::string& key,const std::string& value,ValueType type) {
    if(!opened_) { auto s=Open(); if(!s.ok()) return s; }
    std::lock_guard writer(writer_mutex_);
    if(next_version_==std::numeric_limits<Timestamp>::max()) return Status::IOError("timestamp exhausted");
    InternalKey ik{key,next_version_,type};
    auto s=wal_.Append(ik,value); if(!s.ok()) return s;
    ++next_version_;
    std::shared_ptr<MemTable> active;
    { std::lock_guard lock(state_mutex_); active=active_; }
    active->Put(ik,value);
    if(active->ApproximateMemoryUsage()>=options_.memtable_limit) {
        std::lock_guard lock(state_mutex_);
        if(active_==active) { immutable_.push_back(active_); active_=std::make_shared<MemTable>(); }
    }
    flush_cv_.notify_one();
    return Status::Ok();
}
Status DBImpl::Put(const std::string& k,const std::string& v){return Write(k,v,ValueType::kValue);}
Status DBImpl::Delete(const std::string& k){return Write(k,"",ValueType::kTombstone);}

Status DBImpl::ReadAt(const std::string& key,Timestamp ts,std::string* value) const {
    if(!value) return {Status::Code::kInvalidArgument,"value output must not be null"};
    std::shared_ptr<MemTable> active; std::vector<std::shared_ptr<MemTable>> imm;
    { std::lock_guard lock(state_mutex_); active=active_; imm=immutable_; }
    Timestamp best=0; bool found=false,deleted=false; std::string candidate;
    auto consider=[&](const std::shared_ptr<MemTable>& table) {
        auto it=table->NewIterator(); it->Seek({key,ts,ValueType::kValue});
        if(it->Valid() && it->Key().user_key==key && it->Key().version<=ts && (!found || it->Key().version>best)) {
            best=it->Key().version; found=true; deleted=it->Key().type==ValueType::kTombstone; candidate=std::string(it->Value());
        }
    };
    consider(active); for(auto& table:imm) consider(table);
    if(options_.disk) { InternalKey k; std::string v; auto s=options_.disk->GetAsOf(key,ts,&k,&v);
        if(s.ok() && (!found || k.version>best)){found=true;best=k.version;deleted=k.type==ValueType::kTombstone;candidate=std::move(v);} else if(!s.ok()&&!s.is_not_found()) return s; }
    if(!found||deleted) return Status::NotFound();
    *value=std::move(candidate); return Status::Ok();
}
Status DBImpl::Get(const std::string& k,std::string* v){return GetAsOf(k,std::numeric_limits<Timestamp>::max(),v);}
Status DBImpl::GetAsOf(const std::string& k,Timestamp t,std::string* v){return ReadAt(k,t,v);}
Status DBImpl::Diff(const std::string& k,Timestamp a,Timestamp b,DiffResult* r){
    if(!r)return {Status::Code::kInvalidArgument,"result output must not be null"};
    r->old_value.reset();r->new_value.reset(); std::string v;
    if(ReadAt(k,a,&v).ok())r->old_value=v;
    if(ReadAt(k,b,&v).ok())r->new_value=v;
    r->changed=r->old_value!=r->new_value; return Status::Ok();
}
Status DBImpl::Scan(const std::string& start,const std::string& end,std::vector<std::pair<std::string,std::string>>* out){
    if(!out)return {Status::Code::kInvalidArgument,"results output must not be null"};
    std::map<std::string,std::string> keys;
    if(options_.disk){auto s=options_.disk->Scan(start,end,out);if(!s.ok())return s;for(auto& p:*out)keys.emplace(p.first,p.second);}
    std::shared_ptr<MemTable> active;std::vector<std::shared_ptr<MemTable>> imm;
    {std::lock_guard lock(state_mutex_);active=active_;imm=immutable_;}
    auto collect=[&](const std::shared_ptr<MemTable>& mt){auto it=mt->NewIterator();for(it->SeekToFirst();it->Valid();it->Next()){auto k=it->Key();if(k.user_key>=start&&(end.empty()||k.user_key<end))keys.emplace(k.user_key,std::string{});}};
    collect(active);for(auto&m:imm)collect(m);out->clear();for(auto& p:keys){std::string value;if(ReadAt(p.first,UINT64_MAX,&value).ok())out->emplace_back(p.first,std::move(value));}return Status::Ok();
}
void DBImpl::FlushWorker(){
    std::unique_lock lock(flush_mutex_);
    while(true){flush_cv_.wait(lock,[&]{std::lock_guard state(state_mutex_);return stopping_||!immutable_.empty();});
        {std::lock_guard state(state_mutex_);if(stopping_)return;flushing_=true;}
        lock.unlock();
        for(;;){std::shared_ptr<MemTable> table;{std::lock_guard state(state_mutex_);if(immutable_.empty())break;table=immutable_.front();}
            Status s=Status::Ok();if(options_.disk){auto it=table->NewIterator();s=options_.disk->Flush(*it);} if(!s.ok())break;
            std::lock_guard state(state_mutex_);if(!immutable_.empty()&&immutable_.front()==table)immutable_.erase(immutable_.begin());}
        lock.lock();flushing_=false;flush_cv_.notify_all();
    }
}
Status DBImpl::WaitForFlush(){std::unique_lock lock(flush_mutex_);flush_cv_.wait(lock,[&]{std::lock_guard state(state_mutex_);return immutable_.empty()&&!flushing_;});return Status::Ok();}
} // namespace kvstore
