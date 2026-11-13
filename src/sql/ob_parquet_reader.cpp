/*
 * Copyright (c) 2025 OceanBase.
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
 */
#define USING_LOG_PREFIX SQL
#include "sql/ob_parquet_reader.h"

#include <algorithm>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>
#include "lib/oblog/ob_log.h"

namespace oceanbase { using namespace common; namespace sql {
namespace {

enum CompactType { CT_STOP=0, CT_TRUE=1, CT_FALSE=2, CT_BYTE=3, CT_I16=4, CT_I32=5,
                   CT_I64=6, CT_DOUBLE=7, CT_BINARY=8, CT_LIST=9, CT_SET=10, CT_MAP=11, CT_STRUCT=12 };

uint32_t le32(const unsigned char *p)
{ return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24); }
uint64_t le64(const unsigned char *p)
{ return static_cast<uint64_t>(le32(p)) | (static_cast<uint64_t>(le32(p + 4)) << 32); }

class CompactReader
{
public:
  CompactReader(const unsigned char *p, size_t n) : p_(p), n_(n), pos_(0) {}
  size_t pos() const { return pos_; }
  int byte(unsigned char &v) { if (pos_ >= n_) return OB_INVALID_DATA; v = p_[pos_++]; return OB_SUCCESS; }
  int varint(uint64_t &v) {
    v = 0; int shift = 0; unsigned char b = 0;
    do { if (shift > 63 || OB_SUCCESS != byte(b)) return OB_INVALID_DATA;
         v |= static_cast<uint64_t>(b & 0x7f) << shift; shift += 7; } while (b & 0x80);
    return OB_SUCCESS;
  }
  int sint(int64_t &v) { uint64_t u = 0; int ret = varint(u); if (OB_SUCC(ret)) v = static_cast<int64_t>((u >> 1) ^ (~(u & 1) + 1)); return ret; }
  int binary(std::string &v) {
    uint64_t len = 0; int ret = varint(len);
    if (OB_SUCC(ret) && (len > n_ - pos_ || len > static_cast<uint64_t>(INT32_MAX))) ret = OB_INVALID_DATA;
    if (OB_SUCC(ret)) { v.assign(reinterpret_cast<const char *>(p_ + pos_), static_cast<size_t>(len)); pos_ += len; }
    return ret;
  }
  int field(int16_t &last, int16_t &id, unsigned char &type) {
    unsigned char h = 0; int ret = byte(h); if (OB_FAIL(ret)) return ret;
    type = h & 0x0f; if (type == CT_STOP) { id = 0; return OB_SUCCESS; }
    int delta = h >> 4;
    if (delta) id = static_cast<int16_t>(last + delta);
    else { int64_t x = 0; if (OB_FAIL(sint(x)) || x < 0 || x > INT16_MAX) return OB_INVALID_DATA; id = static_cast<int16_t>(x); }
    last = id; return OB_SUCCESS;
  }
  int list(unsigned char &type, uint64_t &size) {
    unsigned char h = 0; int ret = byte(h); if (OB_FAIL(ret)) return ret;
    type = h & 0x0f; size = h >> 4; if (size == 15) ret = varint(size); return ret;
  }
  int skip(unsigned char type) {
    int ret = OB_SUCCESS; uint64_t n = 0; int64_t x = 0; unsigned char t = 0;
    switch (type) {
      case CT_STOP: case CT_TRUE: case CT_FALSE: break;
      case CT_BYTE: if (pos_ >= n_) ret = OB_INVALID_DATA; else ++pos_; break;
      case CT_I16: case CT_I32: case CT_I64: ret = sint(x); break;
      case CT_DOUBLE: if (n_ - pos_ < 8) ret = OB_INVALID_DATA; else pos_ += 8; break;
      case CT_BINARY: { std::string s; ret = binary(s); break; }
      case CT_LIST: case CT_SET:
        if (OB_FAIL(list(t, n))) break;
        for (uint64_t i = 0; OB_SUCC(ret) && i < n; ++i) ret = skip(t);
        break;
      case CT_MAP: {
        if (OB_FAIL(varint(n))) break; if (n == 0) break;
        unsigned char kt = 0, vt = 0, h = 0; if (OB_FAIL(byte(h))) break; kt = h >> 4; vt = h & 0x0f;
        for (uint64_t i = 0; OB_SUCC(ret) && i < n; ++i) { if (OB_FAIL(skip(kt))) break; ret = skip(vt); }
        break;
      }
      case CT_STRUCT: {
        int16_t last = 0, id = 0; unsigned char ft = 0;
        while (OB_SUCC(ret)) { if (OB_FAIL(field(last,id,ft)) || ft == CT_STOP) break; ret = skip(ft); }
        break;
      }
      default: ret = OB_INVALID_DATA;
    }
    return ret;
  }
private:
  const unsigned char *p_; size_t n_; size_t pos_;
};

struct PqSchema { std::string name; int type=-1; int type_len=0; int repetition=0; int converted=-1; int children=0; };
struct PqChunk { int type=-1; int codec=0; int64_t num_values=0; int64_t data_offset=0; int64_t dict_offset=-1; int64_t total_size=0; std::string name; };
struct PqRowGroup { int64_t rows=0; std::vector<PqChunk> chunks; };
struct PqMeta { std::vector<PqSchema> schema; std::vector<PqRowGroup> groups; int64_t rows=0; };

int read_int(CompactReader &r, int64_t &v) { return r.sint(v); }

int parse_schema(CompactReader &r, PqSchema &s)
{
  int ret = OB_SUCCESS; int16_t last=0,id=0; unsigned char t=0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(r.field(last,id,t)) || t == CT_STOP) break;
    int64_t v=0;
    if (id == 1 && (t == CT_I32 || t == CT_I16)) { if (OB_FAIL(read_int(r,v))) break; s.type=static_cast<int>(v); }
    else if (id == 2 && t == CT_I32) { if (OB_FAIL(read_int(r,v))) break; s.type_len=static_cast<int>(v); }
    else if (id == 3 && (t == CT_I32 || t == CT_I16)) { if (OB_FAIL(read_int(r,v))) break; s.repetition=static_cast<int>(v); }
    else if (id == 4 && t == CT_BINARY) ret = r.binary(s.name);
    else if (id == 5 && t == CT_I32) { if (OB_FAIL(read_int(r,v))) break; s.children=static_cast<int>(v); }
    else if (id == 6 && (t == CT_I32 || t == CT_I16)) { if (OB_FAIL(read_int(r,v))) break; s.converted=static_cast<int>(v); }
    else ret = r.skip(t);
  }
  return ret;
}

int parse_column_meta(CompactReader &r, PqChunk &c)
{
  int ret=OB_SUCCESS; int16_t last=0,id=0; unsigned char t=0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(r.field(last,id,t)) || t == CT_STOP) break; int64_t v=0;
    if (id == 1) { if (OB_FAIL(read_int(r,v))) break; c.type=static_cast<int>(v); }
    else if (id == 3 && t == CT_LIST) {
      unsigned char et=0; uint64_t n=0; if (OB_FAIL(r.list(et,n))) break;
      for (uint64_t i=0; OB_SUCC(ret)&&i<n; ++i) { std::string p; if (OB_FAIL(r.binary(p))) break; c.name=p; }
    } else if (id == 4) { if (OB_FAIL(read_int(r,v))) break; c.codec=static_cast<int>(v); }
    else if (id == 5) ret=read_int(r,c.num_values);
    else if (id == 7) ret=read_int(r,c.total_size);
    else if (id == 9) ret=read_int(r,c.data_offset);
    else if (id == 11) ret=read_int(r,c.dict_offset);
    else ret=r.skip(t);
  }
  return ret;
}

int parse_chunk(CompactReader &r, PqChunk &c)
{
  int ret=OB_SUCCESS; int16_t last=0,id=0; unsigned char t=0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(r.field(last,id,t)) || t==CT_STOP) break;
    if (id==3 && t==CT_STRUCT) ret=parse_column_meta(r,c); else ret=r.skip(t);
  }
  return ret;
}

int parse_row_group(CompactReader &r, PqRowGroup &g)
{
  int ret=OB_SUCCESS; int16_t last=0,id=0; unsigned char t=0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(r.field(last,id,t)) || t==CT_STOP) break;
    if (id==1 && t==CT_LIST) {
      unsigned char et=0; uint64_t n=0; if (OB_FAIL(r.list(et,n)) || et!=CT_STRUCT) { ret=OB_INVALID_DATA; break; }
      for (uint64_t i=0; OB_SUCC(ret)&&i<n; ++i) { PqChunk c; if (OB_FAIL(parse_chunk(r,c))) break; g.chunks.push_back(c); }
    } else if (id==3) ret=read_int(r,g.rows); else ret=r.skip(t);
  }
  return ret;
}

int parse_metadata(const std::vector<unsigned char> &buf, PqMeta &m)
{
  int ret=OB_SUCCESS; CompactReader r(buf.data(),buf.size()); int16_t last=0,id=0; unsigned char t=0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(r.field(last,id,t)) || t==CT_STOP) break;
    if (id==2 && t==CT_LIST) {
      unsigned char et=0; uint64_t n=0; if (OB_FAIL(r.list(et,n)) || et!=CT_STRUCT) { ret=OB_INVALID_DATA; break; }
      for (uint64_t i=0; OB_SUCC(ret)&&i<n; ++i) { PqSchema s; if (OB_FAIL(parse_schema(r,s))) break; m.schema.push_back(s); }
    } else if (id==3) ret=read_int(r,m.rows);
    else if (id==4 && t==CT_LIST) {
      unsigned char et=0; uint64_t n=0; if (OB_FAIL(r.list(et,n)) || et!=CT_STRUCT) { ret=OB_INVALID_DATA; break; }
      for (uint64_t i=0; OB_SUCC(ret)&&i<n; ++i) { PqRowGroup g; if (OB_FAIL(parse_row_group(r,g))) break; m.groups.push_back(g); }
    } else ret=r.skip(t);
  }
  if (OB_SUCC(ret) && (m.schema.size()<2 || m.groups.empty())) ret=OB_INVALID_DATA;
  return ret;
}

int snappy_uncompress(const unsigned char *p, size_t n, std::vector<unsigned char> &out)
{
  size_t pos=0; uint64_t expected=0; int shift=0;
  while (pos<n) { unsigned char b=p[pos++]; expected|=static_cast<uint64_t>(b&0x7f)<<shift; if (!(b&0x80)) break; shift+=7; if (shift>63) return OB_INVALID_DATA; }
  if (expected > static_cast<uint64_t>(INT32_MAX)) return OB_SIZE_OVERFLOW; out.clear(); out.reserve(static_cast<size_t>(expected));
  while (pos<n && out.size()<expected) {
    unsigned char tag=p[pos++]; int kind=tag&3; size_t len=0,off=0;
    if (kind==0) {
      unsigned x=tag>>2; if (x<60) len=x+1; else { unsigned bytes=x-59; if (bytes>4||pos+bytes>n) return OB_INVALID_DATA; uint32_t l=0; for(unsigned i=0;i<bytes;++i) l|=static_cast<uint32_t>(p[pos++])<<(8*i); len=static_cast<size_t>(l)+1; }
      if (pos+len>n || out.size()+len>expected) return OB_INVALID_DATA; out.insert(out.end(),p+pos,p+pos+len); pos+=len;
    } else {
      if (kind==1) { if(pos>=n)return OB_INVALID_DATA; len=4+((tag>>2)&7); off=((tag&0xe0)<<3)|p[pos++]; }
      else if (kind==2) { if(pos+2>n)return OB_INVALID_DATA; len=1+(tag>>2); off=p[pos]|(static_cast<size_t>(p[pos+1])<<8); pos+=2; }
      else { if(pos+4>n)return OB_INVALID_DATA; len=1+(tag>>2); off=le32(p+pos); pos+=4; }
      if (off==0 || off>out.size() || out.size()+len>expected) return OB_INVALID_DATA;
      for(size_t i=0;i<len;++i) out.push_back(out[out.size()-off]);
    }
  }
  return out.size()==expected ? OB_SUCCESS : OB_INVALID_DATA;
}

int decode_hybrid(const unsigned char *p,size_t n,int bw,size_t want,std::vector<uint32_t> &out)
{
  size_t pos=0; out.clear();
  while(pos<n && out.size()<want) {
    uint64_t h=0; int shift=0; unsigned char b=0;
    do { if(pos>=n||shift>63)return OB_INVALID_DATA; b=p[pos++]; h|=static_cast<uint64_t>(b&0x7f)<<shift; shift+=7; } while(b&0x80);
    if ((h&1)==0) { size_t count=static_cast<size_t>(h>>1), bytes=(bw+7)/8; if(pos+bytes>n)return OB_INVALID_DATA; uint32_t v=0; for(size_t i=0;i<bytes;++i)v|=static_cast<uint32_t>(p[pos++])<<(8*i); for(size_t i=0;i<count&&out.size()<want;++i)out.push_back(v); }
    else { size_t count=static_cast<size_t>(h>>1)*8, bits=count*bw, bytes=(bits+7)/8; if(pos+bytes>n)return OB_INVALID_DATA;
      size_t bit=0; for(size_t i=0;i<count&&out.size()<want;++i){ uint32_t v=0; for(int k=0;k<bw;++k,++bit) if(p[pos+bit/8]&(1u<<(bit%8)))v|=1u<<k; out.push_back(v);} pos+=bytes; }
  }
  return out.size()==want ? OB_SUCCESS : OB_INVALID_DATA;
}

void civil_from_days(int z,int &y,unsigned &m,unsigned &d)
{
  z+=719468; int era=(z>=0?z:z-146096)/146097; unsigned doe=static_cast<unsigned>(z-era*146097);
  unsigned yoe=(doe-doe/1460+doe/36524-doe/146096)/365; y=static_cast<int>(yoe)+era*400;
  unsigned doy=doe-(365*yoe+yoe/4-yoe/100); unsigned mp=(5*doy+2)/153; d=doy-(153*mp+2)/5+1; m=mp+(mp<10?3:-9); y+=m<=2;
}

std::string format_date(int days)
{ int y=0; unsigned m=0,d=0; civil_from_days(days,y,m,d); char b[16]; snprintf(b,sizeof(b),"%04d-%02u-%02u",y,m,d); return b; }
std::string format_datetime(int64_t usec)
{
  int64_t sec=usec/1000000; int64_t days=sec/86400, rem=sec%86400; if(rem<0){rem+=86400;--days;}
  int y=0; unsigned m=0,d=0; civil_from_days(static_cast<int>(days),y,m,d); char b[32];
  snprintf(b,sizeof(b),"%04d-%02u-%02u %02lld:%02lld:%02lld",y,m,d,rem/3600,(rem/60)%60,rem%60); return b;
}

int plain_values(const unsigned char *p,size_t n,const PqSchema &s,size_t count,std::vector<std::string> &out)
{
  int ret=OB_SUCCESS; size_t pos=0; out.clear(); out.reserve(count);
  for(size_t i=0;OB_SUCC(ret)&&i<count;++i) {
    std::ostringstream os; os<<std::setprecision(17);
    if(s.type==0) { size_t bit=i; if(bit/8>=n)ret=OB_INVALID_DATA; else out.push_back((p[bit/8]&(1u<<(bit%8)))?"true":"false"); continue; }
    if(s.type==1) { if(pos+4>n){ret=OB_INVALID_DATA;break;} int32_t v=static_cast<int32_t>(le32(p+pos));pos+=4; out.push_back(s.converted==6?format_date(v):std::to_string(v)); }
    else if(s.type==2) { if(pos+8>n){ret=OB_INVALID_DATA;break;} int64_t v=static_cast<int64_t>(le64(p+pos));pos+=8; if(s.converted==9)out.push_back(format_datetime(v*1000)); else if(s.converted==10)out.push_back(format_datetime(v)); else out.push_back(std::to_string(v)); }
    else if(s.type==3) { if(pos+12>n){ret=OB_INVALID_DATA;break;} uint64_t ns=le64(p+pos); int days=static_cast<int>(le32(p+pos+8))-2440588; pos+=12; out.push_back(format_datetime(static_cast<int64_t>(days)*86400000000LL+static_cast<int64_t>(ns/1000))); }
    else if(s.type==4) { if(pos+4>n){ret=OB_INVALID_DATA;break;} float v; MEMCPY(&v,p+pos,4);pos+=4;os<<v;out.push_back(os.str()); }
    else if(s.type==5) { if(pos+8>n){ret=OB_INVALID_DATA;break;} double v;MEMCPY(&v,p+pos,8);pos+=8;os<<v;out.push_back(os.str()); }
    else if(s.type==6) { if(pos+4>n){ret=OB_INVALID_DATA;break;} uint32_t len=le32(p+pos);pos+=4;if(pos+len>n){ret=OB_INVALID_DATA;break;}out.push_back(std::string(reinterpret_cast<const char*>(p+pos),len));pos+=len; }
    else if(s.type==7) { if(s.type_len<=0||pos+static_cast<size_t>(s.type_len)>n){ret=OB_INVALID_DATA;break;}out.push_back(std::string(reinterpret_cast<const char*>(p+pos),s.type_len));pos+=s.type_len; }
    else ret=OB_NOT_SUPPORTED;
  }
  return ret;
}

struct PageHeader { int type=-1,uncompressed=0,compressed=0,num_values=0,encoding=0,dict_values=0; };
int parse_data_header(CompactReader &r,PageHeader &h)
{ int ret=OB_SUCCESS;int16_t last=0,id=0;unsigned char t=0;while(OB_SUCC(ret)){if(OB_FAIL(r.field(last,id,t))||t==CT_STOP)break;int64_t v=0;if(id==1){ret=read_int(r,v);h.num_values=static_cast<int>(v);}else if(id==2){ret=read_int(r,v);h.encoding=static_cast<int>(v);}else ret=r.skip(t);}return ret; }
int parse_dict_header(CompactReader &r,PageHeader &h)
{ int ret=OB_SUCCESS;int16_t last=0,id=0;unsigned char t=0;while(OB_SUCC(ret)){if(OB_FAIL(r.field(last,id,t))||t==CT_STOP)break;int64_t v=0;if(id==1){ret=read_int(r,v);h.dict_values=static_cast<int>(v);}else if(id==2){ret=read_int(r,v);h.encoding=static_cast<int>(v);}else ret=r.skip(t);}return ret; }
int parse_page_header(const unsigned char *p,size_t n,PageHeader &h,size_t &used)
{ int ret=OB_SUCCESS;CompactReader r(p,n);int16_t last=0,id=0;unsigned char t=0;while(OB_SUCC(ret)){if(OB_FAIL(r.field(last,id,t))||t==CT_STOP)break;int64_t v=0;if(id==1){ret=read_int(r,v);h.type=static_cast<int>(v);}else if(id==2){ret=read_int(r,v);h.uncompressed=static_cast<int>(v);}else if(id==3){ret=read_int(r,v);h.compressed=static_cast<int>(v);}else if(id==5&&t==CT_STRUCT)ret=parse_data_header(r,h);else if(id==7&&t==CT_STRUCT)ret=parse_dict_header(r,h);else ret=r.skip(t);}used=r.pos();return ret; }

ObObjType schema_type(const PqSchema &s)
{
  if(s.converted==6)return ObDateType; if(s.converted==9||s.converted==10||s.type==3)return ObDateTimeType;
  if(s.type==0)return ObTinyIntType; if(s.type==1||s.type==2)return ObIntType; if(s.type==4||s.type==5)return ObDoubleType; return ObVarcharType;
}

} // anonymous namespace

struct ObParquetReader::Impl
{
  std::string path_; std::ifstream in_; PqMeta meta_; size_t group_=0,row_=0;
  std::vector<int64_t> projected_columns_;
  std::vector<std::vector<std::string> > values_; std::vector<std::vector<bool> > nulls_;

  int load_meta() {
    int ret=OB_SUCCESS; in_.seekg(0,std::ios::end); std::streamoff size=in_.tellg();
    if(size<12)return OB_INVALID_DATA; unsigned char tail[8];in_.seekg(size-8);in_.read(reinterpret_cast<char*>(tail),8);
    if(in_.gcount()!=8||MEMCMP(tail+4,"PAR1",4)!=0)return OB_INVALID_DATA;uint32_t len=le32(tail);if(len>static_cast<uint64_t>(size-12))return OB_INVALID_DATA;
    std::vector<unsigned char> b(len);in_.seekg(size-8-len);in_.read(reinterpret_cast<char*>(b.data()),len);if(static_cast<uint32_t>(in_.gcount())!=len)return OB_IO_ERROR;
    return parse_metadata(b,meta_);
  }

  int decode_chunk(const PqSchema &s,const PqChunk &c,int64_t rows,std::vector<std::string>&vals,std::vector<bool>&nulls) {
    int ret=OB_SUCCESS;vals.clear();nulls.clear();std::vector<std::string> dict;int64_t seen=0;int64_t off=c.dict_offset>=0?std::min(c.dict_offset,c.data_offset):c.data_offset;
    while(OB_SUCC(ret)&&seen<c.num_values) {
      unsigned char hb[65536];in_.clear();in_.seekg(off);in_.read(reinterpret_cast<char*>(hb),sizeof(hb));size_t got=static_cast<size_t>(in_.gcount());PageHeader ph;size_t hs=0;
      if(got==0||OB_FAIL(parse_page_header(hb,got,ph,hs))||ph.compressed<0){ret=OB_INVALID_DATA;break;}off+=hs;
      std::vector<unsigned char> comp(ph.compressed);in_.clear();in_.seekg(off);in_.read(reinterpret_cast<char*>(comp.data()),ph.compressed);if(in_.gcount()!=ph.compressed){ret=OB_IO_ERROR;break;}off+=ph.compressed;
      std::vector<unsigned char> page;if(c.codec==0)page.swap(comp);else if(c.codec==1)ret=snappy_uncompress(comp.data(),comp.size(),page);else ret=OB_NOT_SUPPORTED;
      if(OB_FAIL(ret))break;if(ph.uncompressed!=static_cast<int>(page.size())){ret=OB_INVALID_DATA;break;}
      if(ph.type==2) { if(ph.encoding!=0){ret=OB_NOT_SUPPORTED;break;}ret=plain_values(page.data(),page.size(),s,ph.dict_values,dict); }
      else if(ph.type==0) {
        size_t pos=0;std::vector<uint32_t> defs;if(s.repetition==1){if(page.size()<4){ret=OB_INVALID_DATA;break;}uint32_t dl=le32(page.data());pos=4;if(pos+dl>page.size()){ret=OB_INVALID_DATA;break;}ret=decode_hybrid(page.data()+pos,dl,1,ph.num_values,defs);pos+=dl;}else defs.assign(ph.num_values,1);
        if(OB_FAIL(ret))break;size_t nonnull=0;for(size_t i=0;i<defs.size();++i)if(defs[i])++nonnull;std::vector<std::string> decoded;
        if(ph.encoding==0)ret=plain_values(page.data()+pos,page.size()-pos,s,nonnull,decoded);
        else if(ph.encoding==2||ph.encoding==8){if(pos>=page.size()){ret=OB_INVALID_DATA;break;}int bw=page[pos++];std::vector<uint32_t> idx;ret=decode_hybrid(page.data()+pos,page.size()-pos,bw,nonnull,idx);for(size_t i=0;OB_SUCC(ret)&&i<idx.size();++i){if(idx[i]>=dict.size())ret=OB_INVALID_DATA;else decoded.push_back(dict[idx[i]]);}}
        else ret=OB_NOT_SUPPORTED;
        size_t vi=0;for(size_t i=0;OB_SUCC(ret)&&i<defs.size();++i){if(defs[i]){vals.push_back(decoded[vi++]);nulls.push_back(false);}else{vals.push_back(std::string());nulls.push_back(true);}}seen+=ph.num_values;
      } else if(ph.type==1) { } else ret=OB_NOT_SUPPORTED;
    }
    if(OB_SUCC(ret)&&(vals.size()!=static_cast<size_t>(rows)||nulls.size()!=static_cast<size_t>(rows)))ret=OB_INVALID_DATA;return ret;
  }

  int load_group() {
    int ret=OB_SUCCESS;if(group_>=meta_.groups.size())return OB_ITER_END;const PqRowGroup &g=meta_.groups[group_++];size_t n=meta_.schema.size()-1;
    if(g.chunks.size()!=n)return OB_NOT_SUPPORTED;values_.assign(n,std::vector<std::string>());nulls_.assign(n,std::vector<bool>());
    for(size_t i=0;OB_SUCC(ret)&&i<n;++i){
      const PqSchema&s=meta_.schema[i+1];
      const bool projected=projected_columns_.empty()
          || std::find(projected_columns_.begin(),projected_columns_.end(),static_cast<int64_t>(i))!=projected_columns_.end();
      if(s.children>0||s.repetition==2)ret=OB_NOT_SUPPORTED;
      else if(projected)ret=decode_chunk(s,g.chunks[i],g.rows,values_[i],nulls_[i]);
      else {values_[i].assign(static_cast<size_t>(g.rows),std::string());nulls_[i].assign(static_cast<size_t>(g.rows),true);}
    }row_=0;return ret;
  }
};

ObParquetReader::ObParquetReader():impl_(NULL){}
ObParquetReader::~ObParquetReader(){close();}
int ObParquetReader::open(const std::string &path,const std::vector<int64_t>&projected_columns)
{ close();impl_=new(std::nothrow)Impl();if(!impl_)return OB_ALLOCATE_MEMORY_FAILED;impl_->path_=path;impl_->projected_columns_=projected_columns;impl_->in_.open(path.c_str(),std::ios::in|std::ios::binary);if(!impl_->in_.is_open()){close();return OB_FILE_NOT_EXIST;}int ret=impl_->load_meta();if(OB_FAIL(ret))close();return ret; }
int ObParquetReader::get_schema(std::vector<ObFileTableColumn>&cols)const
{ if(!impl_)return OB_NOT_INIT;cols.clear();for(size_t i=1;i<impl_->meta_.schema.size();++i){const PqSchema&s=impl_->meta_.schema[i];if(s.children>0||s.repetition==2)return OB_NOT_SUPPORTED;ObFileTableColumn c;c.name_=s.name;c.type_=schema_type(s);c.max_length_=OB_MAX_VARCHAR_LENGTH;c.has_null_=(s.repetition==1);cols.push_back(c);}return cols.empty()?OB_INVALID_DATA:OB_SUCCESS; }
int ObParquetReader::next(std::vector<std::string>&v,std::vector<bool>&n)
{ if(!impl_)return OB_NOT_INIT;int ret=OB_SUCCESS;while(impl_->values_.empty()||(impl_->values_.size()>0&&impl_->row_>=impl_->values_[0].size())){ret=impl_->load_group();if(OB_FAIL(ret))return ret;}v.resize(impl_->values_.size());n.resize(impl_->nulls_.size());for(size_t i=0;i<v.size();++i){v[i]=impl_->values_[i][impl_->row_];n[i]=impl_->nulls_[i][impl_->row_];}++impl_->row_;return ret; }
int ObParquetReader::rescan(){if(!impl_)return OB_NOT_INIT;impl_->group_=0;impl_->row_=0;impl_->values_.clear();impl_->nulls_.clear();return OB_SUCCESS;}
void ObParquetReader::close(){if(impl_){if(impl_->in_.is_open())impl_->in_.close();delete impl_;impl_=NULL;}}

} } // oceanbase::sql
