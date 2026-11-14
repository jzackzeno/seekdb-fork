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
#include "sql/ob_file_sql_utils.h"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <sys/stat.h>
#ifndef _WIN32
#include <glob.h>
#include <unistd.h>
#else
#include <direct.h>
#endif
#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_file_table.h"
#include "sql/ob_parquet_reader.h"

namespace oceanbase { using namespace common; namespace sql {
namespace {
const int64_t MAX_FILE_SQL_COLUMNS = 4096;
const int64_t MAX_FILE_SQL_FIELD_SIZE = 64L * 1024L * 1024L;

struct JsonScalar {
  JsonScalar() : is_null_(false), is_string_(false) {}
  std::string value_; bool is_null_; bool is_string_;
};

bool valid_utf8(const std::string &s)
{
  size_t i = 0;
  while (i < s.size()) {
    const unsigned char c = static_cast<unsigned char>(s[i++]);
    if (c <= 0x7f) continue;
    int need = 0;
    unsigned int cp = 0;
    if ((c & 0xe0) == 0xc0) { need = 1; cp = c & 0x1f; if (cp < 2) return false; }
    else if ((c & 0xf0) == 0xe0) { need = 2; cp = c & 0x0f; }
    else if ((c & 0xf8) == 0xf0) { need = 3; cp = c & 0x07; }
    else return false;
    if (i + need > s.size()) return false;
    for (int j = 0; j < need; ++j) {
      const unsigned char trail = static_cast<unsigned char>(s[i++]);
      if ((trail & 0xc0) != 0x80) return false;
      cp = (cp << 6) | (trail & 0x3f);
    }
    if ((need == 2 && cp < 0x800) || (need == 3 && cp < 0x10000)
        || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) return false;
  }
  return true;
}

void skip_ws(const std::string &s, size_t &p)
{
  while (p < s.size() && (s[p] == ' ' || s[p] == '\t' || s[p] == '\r' || s[p] == '\n')) ++p;
}

int append_utf8(unsigned cp, std::string &out)
{
  if (cp <= 0x7f) out.push_back(static_cast<char>(cp));
  else if (cp <= 0x7ff) {
    out.push_back(static_cast<char>(0xc0 | (cp >> 6)));
    out.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
  } else if (cp <= 0xffff) {
    out.push_back(static_cast<char>(0xe0 | (cp >> 12)));
    out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
    out.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
  } else return OB_INVALID_DATA;
  return OB_SUCCESS;
}

int parse_json_string(const std::string &s, size_t &p, std::string &out)
{
  int ret = OB_SUCCESS; out.clear();
  if (p >= s.size() || s[p++] != '"') ret = OB_INVALID_DATA;
  while (OB_SUCC(ret) && p < s.size()) {
    char c = s[p++];
    if (c == '"') return OB_SUCCESS;
    if (static_cast<unsigned char>(c) < 0x20) ret = OB_INVALID_DATA;
    else if (c != '\\') out.push_back(c);
    else if (p >= s.size()) ret = OB_INVALID_DATA;
    else {
      char e = s[p++];
      switch (e) {
        case '"': out.push_back('"'); break; case '\\': out.push_back('\\'); break;
        case '/': out.push_back('/'); break; case 'b': out.push_back('\b'); break;
        case 'f': out.push_back('\f'); break; case 'n': out.push_back('\n'); break;
        case 'r': out.push_back('\r'); break; case 't': out.push_back('\t'); break;
        case 'u': {
          if (p + 4 > s.size()) ret = OB_INVALID_DATA;
          unsigned cp = 0;
          for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
            char h = s[p++]; cp <<= 4;
            if (h >= '0' && h <= '9') cp += h - '0';
            else if (h >= 'a' && h <= 'f') cp += h - 'a' + 10;
            else if (h >= 'A' && h <= 'F') cp += h - 'A' + 10;
            else ret = OB_INVALID_DATA;
          }
          if (OB_SUCC(ret) && cp >= 0xd800 && cp <= 0xdbff) {
            if (p + 6 > s.size() || s[p] != '\\' || s[p + 1] != 'u') {
              ret = OB_INVALID_DATA;
            } else {
              p += 2;
              unsigned int low = 0;
              for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
                char h = s[p++]; low <<= 4;
                if (h >= '0' && h <= '9') low += h - '0';
                else if (h >= 'a' && h <= 'f') low += h - 'a' + 10;
                else if (h >= 'A' && h <= 'F') low += h - 'A' + 10;
                else ret = OB_INVALID_DATA;
              }
              if (low < 0xdc00 || low > 0xdfff) ret = OB_INVALID_DATA;
              else cp = 0x10000 + ((cp - 0xd800) << 10) + (low - 0xdc00);
            }
          } else if (OB_SUCC(ret) && cp >= 0xdc00 && cp <= 0xdfff) {
            ret = OB_INVALID_DATA;
          }
          if (OB_SUCC(ret)) ret = append_utf8(cp, out);
          break;
        }
        default: ret = OB_INVALID_DATA;
      }
    }
  }
  return OB_SUCC(ret) ? OB_INVALID_DATA : ret;
}

int parse_flat_object(const std::string &line,
                      std::vector<std::pair<std::string, JsonScalar> > &fields)
{
  int ret = OB_SUCCESS; size_t p = 0; fields.clear(); skip_ws(line, p);
  if (p >= line.size() || line[p++] != '{') ret = OB_INVALID_DATA;
  skip_ws(line, p);
  if (OB_SUCC(ret) && p < line.size() && line[p] == '}') ++p;
  else while (OB_SUCC(ret)) {
    std::string key; JsonScalar val; skip_ws(line, p);
    if (OB_FAIL(parse_json_string(line, p, key))) {}
    else { skip_ws(line, p); if (p >= line.size() || line[p++] != ':') ret = OB_INVALID_DATA; }
    if (OB_SUCC(ret)) {
      skip_ws(line, p);
      if (p >= line.size() || line[p] == '{' || line[p] == '[') ret = OB_NOT_SUPPORTED;
      else if (line[p] == '"') { val.is_string_ = true; ret = parse_json_string(line, p, val.value_); }
      else {
        size_t start = p;
        while (p < line.size() && line[p] != ',' && line[p] != '}' &&
               line[p] != ' ' && line[p] != '\t' && line[p] != '\r' && line[p] != '\n') ++p;
        val.value_.assign(line, start, p - start);
        if (val.value_ == "null") val.is_null_ = true;
        else if (val.value_ != "true" && val.value_ != "false") {
          char *end = NULL; errno = 0; (void)std::strtod(val.value_.c_str(), &end);
          if (val.value_.empty() || end == NULL || *end != '\0' || errno == ERANGE) ret = OB_INVALID_DATA;
        }
      }
    }
    if (OB_SUCC(ret)) {
      fields.push_back(std::make_pair(key, val)); skip_ws(line, p);
      if (p >= line.size()) ret = OB_INVALID_DATA;
      else if (line[p] == ',') ++p;
      else if (line[p] == '}') { ++p; break; }
      else ret = OB_INVALID_DATA;
    }
  }
  skip_ws(line, p); if (OB_SUCC(ret) && p != line.size()) ret = OB_INVALID_DATA;
  if (OB_SUCC(ret) && !valid_utf8(line)) ret = OB_INVALID_DATA;
  return ret;
}

std::string lower(const std::string &s)
{
  std::string r(s); std::transform(r.begin(), r.end(), r.begin(), ::tolower); return r;
}

bool parse_bool(const std::string &s, bool &v)
{
  std::string x = lower(s);
  if (x == "true" || x == "1" || x == "on") v = true;
  else if (x == "false" || x == "0" || x == "off") v = false;
  else return false;
  return true;
}

int civil_days(int y, unsigned m, unsigned d)
{
  y -= m <= 2; int era = (y >= 0 ? y : y - 399) / 400;
  unsigned yoe = static_cast<unsigned>(y - era * 400);
  unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + static_cast<int>(doe) - 719468;
}

bool valid_ymd(int y, int m, int d)
{
  static const int md[] = {31,28,31,30,31,30,31,31,30,31,30,31};
  if (y < 1 || y > 9999 || m < 1 || m > 12 || d < 1) return false;
  int maxd = md[m - 1];
  if (m == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) ++maxd;
  return d <= maxd;
}
} // anonymous

ObFileTableOptions::ObFileTableOptions()
  : delimiter_(','), quote_('"'), escape_('"'), header_(true), null_string_(""), format_(""), sample_size_(-1) {}

bool ObFileSqlUtils::is_file_expr_type(ObExprOperatorType t) { return is_file_table_expr_type(t); }
ObFileTableFormat ObFileSqlUtils::format_from_expr_type(ObExprOperatorType t)
{
  return t == T_FUN_SYS_READ_CSV ? FILE_FORMAT_CSV
       : (t == T_FUN_SYS_READ_JSON || t == T_FUN_SYS_READ_NDJSON) ? FILE_FORMAT_JSONL
       : t == T_FUN_SYS_READ_PARQUET ? FILE_FORMAT_PARQUET : FILE_FORMAT_GLOB;
}

int ObFileSqlUtils::parse_options(const std::vector<std::string> &args, ObFileTableOptions &o)
{
  int ret = OB_SUCCESS; o = ObFileTableOptions();
  if (args.empty() || args[0].empty()) ret = OB_INVALID_ARGUMENT; else o.path_ = args[0];
  for (size_t i = 1; OB_SUCC(ret) && i < args.size(); i += 2) {
    if (i + 1 >= args.size()) ret = OB_ERR_PARSE_SQL;
    else {
      std::string n = lower(args[i]); const std::string &v = args[i + 1];
      if (n == "delimiter" || n == "delim") { if (v.size() != 1) ret = OB_INVALID_ARGUMENT; else o.delimiter_ = v[0]; }
      else if (n == "quote") { if (v.size() != 1) ret = OB_INVALID_ARGUMENT; else o.quote_ = v[0]; }
      else if (n == "escape") { if (v.size() != 1) ret = OB_INVALID_ARGUMENT; else o.escape_ = v[0]; }
      else if (n == "header") { if (!parse_bool(v, o.header_)) ret = OB_INVALID_ARGUMENT; }
      else if (n == "null" || n == "nullstr" || n == "null_string") o.null_string_ = v;
      else if (n == "format") o.format_ = lower(v);
      else if (n == "sample_size") {
        char *end = NULL; o.sample_size_ = std::strtoll(v.c_str(), &end, 10);
        if (end == NULL || *end != '\0' || o.sample_size_ != -1) ret = OB_NOT_SUPPORTED;
      } else ret = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCC(ret) && (o.delimiter_ == o.quote_ || o.delimiter_ == '\r' || o.delimiter_ == '\n')) ret = OB_INVALID_ARGUMENT;
  return ret;
}

int ObFileSqlUtils::canonicalize_path(const std::string &path, std::string &out)
{
  char buf[OB_MAX_FILE_NAME_LENGTH];
#ifdef _WIN32
  if (_fullpath(buf, path.c_str(), sizeof(buf)) == NULL) return OB_FILE_NOT_EXIST;
#else
  if (realpath(path.c_str(), buf) == NULL) return OB_FILE_NOT_EXIST;
#endif
  out.assign(buf); return OB_SUCCESS;
}

int ObFileSqlUtils::check_secure_path(const std::string &root, const std::string &path, bool pattern)
{
  if (root.empty() || lower(root) == "null") return OB_ERR_NO_PRIVILEGE;
  std::string check = path;
  if (pattern) { size_t slash = check.find_last_of("/\\"); check = slash == std::string::npos ? "." : check.substr(0, slash); }
  std::string real_root, real_path;
  int ret = canonicalize_path(root, real_root);
  if (OB_SUCC(ret)) ret = canonicalize_path(check, real_path);
  if (OB_SUCC(ret)) {
#ifdef _WIN32
    real_root = lower(real_root); real_path = lower(real_path);
#endif
    while (real_root.size() > 1 && (real_root.back() == '/' || real_root.back() == '\\')) real_root.pop_back();
    const bool root_fs = real_root == "/";
    const bool prefix = real_path.compare(0, real_root.size(), real_root) == 0;
    const bool boundary = real_path.size() == real_root.size() || root_fs ||
      (real_path.size() > real_root.size() && (real_path[real_root.size()] == '/' || real_path[real_root.size()] == '\\'));
    if (!prefix || !boundary) ret = OB_ERR_NO_PRIVILEGE;
  }
  return ret;
}

int ObFileSqlUtils::get_file_snapshot(const std::string &path, ObFileSnapshot &snap)
{
  struct stat st;
  if (stat(path.c_str(), &st) != 0) return OB_FILE_NOT_EXIST;
#ifdef _WIN32
  if ((st.st_mode & _S_IFMT) != _S_IFREG) return OB_FILE_NOT_EXIST;
#else
  if (!S_ISREG(st.st_mode)) return OB_FILE_NOT_EXIST;
#endif
  snap.size_ = static_cast<int64_t>(st.st_size); snap.mtime_ = static_cast<int64_t>(st.st_mtime);
#if defined(__linux__)
  snap.mtime_nsec_ = static_cast<int64_t>(st.st_mtim.tv_nsec);
#elif defined(__APPLE__)
  snap.mtime_nsec_ = static_cast<int64_t>(st.st_mtimespec.tv_nsec);
#endif
  return OB_SUCCESS;
}

ObObjType ObFileSqlUtils::widen_type(ObObjType a, ObObjType b)
{
  if (a == ObNullType) return b; if (b == ObNullType || a == b) return a;
  if ((a == ObIntType && b == ObDoubleType) || (a == ObDoubleType && b == ObIntType)) return ObDoubleType;
  if ((a == ObDateType && b == ObDateTimeType) || (a == ObDateTimeType && b == ObDateType)) return ObDateTimeType;
  // BOOLEAN is stored as TinyInt: true/false mixed with integers is still
  // numeric (1/0), and mixed with doubles is a double. Without this a
  // column holding e.g. true, 1, false widened to VARCHAR and lost its
  // numeric/boolean meaning.
  if ((a == ObTinyIntType && b == ObIntType) || (a == ObIntType && b == ObTinyIntType)) return ObIntType;
  if ((a == ObTinyIntType && b == ObDoubleType) || (a == ObDoubleType && b == ObTinyIntType)) return ObDoubleType;
  return ObVarcharType;
}

bool ObFileSqlUtils::parse_date(const std::string &s, int32_t &days)
{
  if (s.size() != 10 || s[4] != '-' || s[7] != '-') return false;
  for (size_t i = 0; i < s.size(); ++i) if (i != 4 && i != 7 && (s[i] < '0' || s[i] > '9')) return false;
  int y = std::atoi(s.substr(0,4).c_str()), m = std::atoi(s.substr(5,2).c_str()), d = std::atoi(s.substr(8,2).c_str());
  if (!valid_ymd(y,m,d)) return false; days = static_cast<int32_t>(civil_days(y,m,d)); return true;
}

bool ObFileSqlUtils::parse_datetime(const std::string &s, int64_t &usec)
{
  if (s.size() != 19 || (s[10] != ' ' && s[10] != 'T') || s[13] != ':' || s[16] != ':') return false;
  int32_t days = 0; if (!parse_date(s.substr(0,10), days)) return false;
  for (size_t i = 11; i < s.size(); ++i) if (i != 13 && i != 16 && (s[i] < '0' || s[i] > '9')) return false;
  int h = std::atoi(s.substr(11,2).c_str()), m = std::atoi(s.substr(14,2).c_str()), sec = std::atoi(s.substr(17,2).c_str());
  if (h > 23 || m > 59 || sec > 59) return false;
  usec = (static_cast<int64_t>(days) * 86400 + h * 3600 + m * 60 + sec) * 1000000; return true;
}

ObObjType ObFileSqlUtils::infer_scalar_type(const std::string &s, bool json_string)
{
  int32_t days = 0; int64_t usec = 0;
  if (json_string) {
    if (parse_date(s, days)) return ObDateType; if (parse_datetime(s, usec)) return ObDateTimeType; return ObVarcharType;
  }
  if (s == "true" || s == "false") return ObTinyIntType;
  char *end = NULL; errno = 0; (void)std::strtoll(s.c_str(), &end, 10);
  if (!s.empty() && end != NULL && *end == '\0' && errno != ERANGE) return ObIntType;
  errno = 0; double d = std::strtod(s.c_str(), &end);
  if (!s.empty() && end != NULL && *end == '\0' && errno != ERANGE && std::isfinite(d)) return ObDoubleType;
  if (parse_date(s, days)) return ObDateType; if (parse_datetime(s, usec)) return ObDateTimeType; return ObVarcharType;
}

int ObFileSqlUtils::infer_schema(ObFileTableFormat fmt, const ObFileTableOptions &o,
                                 std::vector<ObFileTableColumn> &cols, ObFileSnapshot &snap)
{
  int ret = OB_SUCCESS; cols.clear();
  // Accept a format option that matches the function being called, not only
  // the JSONL spellings: read_csv(..., format='csv') and
  // read_parquet(..., format='parquet') must not fail here.
  const bool format_ok = o.format_.empty()
      || (fmt == FILE_FORMAT_CSV && o.format_ == "csv")
      || (fmt == FILE_FORMAT_PARQUET && o.format_ == "parquet")
      || (fmt == FILE_FORMAT_JSONL
          && (o.format_ == "newline_delimited" || o.format_ == "ndjson"
              || o.format_ == "json" || o.format_ == "jsonl"))
      || (fmt == FILE_FORMAT_GLOB && o.format_ == "glob");
  if (!format_ok) {
    ret = OB_INVALID_ARGUMENT;
  } else if (fmt == FILE_FORMAT_GLOB) {
    ObFileTableColumn c; c.name_ = "file"; c.type_ = ObVarcharType; c.max_length_ = OB_MAX_FILE_NAME_LENGTH; cols.push_back(c);
  } else if (fmt == FILE_FORMAT_PARQUET) {
    ObParquetReader reader;
    if (OB_FAIL(get_file_snapshot(o.path_, snap))) {
    } else if (OB_FAIL(reader.open(o.path_))) {
    } else {
      ret = reader.get_schema(cols);
      if (OB_SUCC(ret) && cols.size() > static_cast<size_t>(MAX_FILE_SQL_COLUMNS)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("too many columns in Parquet file", K(ret), K(cols.size()));
      }
    }
  } else if (OB_FAIL(get_file_snapshot(o.path_, snap))) {
  } else if (fmt == FILE_FORMAT_CSV) {
    ObFileTableScanner scanner; std::vector<std::string> row; std::vector<bool> nulls;
    std::vector<ObFileTableColumn> empty_cols;
    if (OB_FAIL(scanner.open(fmt, o, empty_cols))) {
    } else if (OB_FAIL(scanner.next(row, nulls))) {
      if (ret == OB_ITER_END) ret = OB_INVALID_DATA;
    } else {
      for (size_t i = 0; i < row.size(); ++i) {
        ObFileTableColumn c; c.name_ = o.header_ ? row[i] : "column" + std::to_string(i + 1);
        if (c.name_.empty()) c.name_ = "column" + std::to_string(i + 1); cols.push_back(c);
      }
      if (cols.size() > static_cast<size_t>(MAX_FILE_SQL_COLUMNS)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("too many columns in CSV file", K(ret), K(cols.size()));
      }
      std::set<std::string> names;
      for (size_t i = 0; i < cols.size(); ++i) {
        const std::string base = cols[i].name_;
        std::string candidate = base;
        int64_t suffix = 2;
        while (!names.insert(lower(candidate)).second) {
          candidate = base + "__" + std::to_string(suffix++);
        }
        cols[i].name_ = candidate;
      }
      if (OB_SUCC(ret) && !o.header_) {
        for (size_t i = 0; i < row.size(); ++i) if (!nulls[i]) {
          cols[i].type_ = widen_type(cols[i].type_, infer_scalar_type(row[i], false));
          cols[i].max_length_ = std::max(cols[i].max_length_, static_cast<int64_t>(row[i].size()));
        } else if (i < cols.size()) {
          cols[i].has_null_ = true;
        }
      }
      while (OB_SUCC(ret)) {
        int tmp = scanner.next(row, nulls);
        if (tmp == OB_ITER_END) break; if (tmp != OB_SUCCESS) { ret = tmp; break; }
        if (row.size() != cols.size()) { ret = OB_INVALID_DATA; break; }
        for (size_t i = 0; i < row.size(); ++i) if (!nulls[i]) {
          cols[i].type_ = widen_type(cols[i].type_, infer_scalar_type(row[i], false));
          cols[i].max_length_ = std::max(cols[i].max_length_, static_cast<int64_t>(row[i].size()));
        } else {
          cols[i].has_null_ = true;
        }
      }
    }
  } else {
    std::ifstream in(o.path_.c_str(), std::ios::in | std::ios::binary); std::string line;
    std::map<std::string, size_t> indexes; std::map<std::string, int64_t> first_seen; int64_t line_no = 0;
    if (!in.is_open()) ret = OB_FILE_NOT_EXIST;
    while (OB_SUCC(ret) && std::getline(in, line)) {
      ++line_no; if (!line.empty() && line.back() == '\r') line.pop_back(); if (line.empty()) continue;
      std::vector<std::pair<std::string, JsonScalar> > fields;
      if (OB_FAIL(parse_flat_object(line, fields))) { LOG_WARN("invalid JSONL row", K(ret), K(line_no)); }
      for (size_t i = 0; OB_SUCC(ret) && i < fields.size(); ++i) {
        std::map<std::string,size_t>::iterator it = indexes.find(fields[i].first);
        if (it == indexes.end()) {
          if (cols.size() >= static_cast<size_t>(MAX_FILE_SQL_COLUMNS)) ret = OB_SIZE_OVERFLOW;
          else { ObFileTableColumn c; c.name_ = fields[i].first; cols.push_back(c); indexes[c.name_] = cols.size() - 1; first_seen[c.name_] = line_no; it = indexes.find(c.name_); }
        }
        if (OB_SUCC(ret) && fields[i].second.is_null_) {
          cols[it->second].has_null_ = true;
        }
        if (OB_SUCC(ret) && !fields[i].second.is_null_) {
          ObFileTableColumn &c = cols[it->second];
          c.type_ = widen_type(c.type_, infer_scalar_type(fields[i].second.value_, fields[i].second.is_string_));
          c.max_length_ = std::max(c.max_length_, static_cast<int64_t>(fields[i].second.value_.size()));
        }
      }
      if (OB_SUCC(ret)) {
        // Columns absent from this row are NULL for it.
        for (size_t i = 0; i < cols.size(); ++i) {
          bool present = false;
          for (size_t j = 0; !present && j < fields.size(); ++j) present = (fields[j].first == cols[i].name_);
          if (!present) cols[i].has_null_ = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      // A column first seen after row 1 was missing in all earlier rows.
      for (size_t i = 0; i < cols.size(); ++i) {
        std::map<std::string, int64_t>::const_iterator it = first_seen.find(cols[i].name_);
        if (it != first_seen.end() && it->second > 1) cols[i].has_null_ = true;
      }
    }
    if (OB_SUCC(ret) && cols.empty()) ret = OB_INVALID_DATA;
  }
  if (OB_SUCC(ret)) {
    // An all-NULL / all-missing column has no observed value to infer from;
    // report it deterministically as VARCHAR instead of NULL type.
    for (size_t i = 0; i < cols.size(); ++i) {
      if (cols[i].type_ == ObNullType) cols[i].type_ = ObVarcharType;
    }
  }
  return ret;
}

ObFileTableScanner::ObFileTableScanner()
  : format_(FILE_FORMAT_CSV), row_number_(0), glob_pos_(0), parquet_reader_(NULL) {}
ObFileTableScanner::~ObFileTableScanner() { close(); }

int ObFileTableScanner::open(ObFileTableFormat fmt, const ObFileTableOptions &o,
                             const std::vector<ObFileTableColumn> &cols,
                             const std::vector<int64_t> &projected_columns)
{
  int ret = OB_SUCCESS; close(); format_ = fmt; options_ = o; columns_ = cols;
  projected_columns_ = projected_columns; row_number_ = 0; glob_pos_ = 0;
  if (format_ == FILE_FORMAT_GLOB) {
#ifndef _WIN32
    glob_t g; MEMSET(&g, 0, sizeof(g)); int gr = ::glob(o.path_.c_str(), GLOB_NOSORT, NULL, &g);
    if (gr != 0 && gr != GLOB_NOMATCH) ret = OB_IO_ERROR;
    else { for (size_t i = 0; i < g.gl_pathc; ++i) glob_files_.push_back(g.gl_pathv[i]); std::sort(glob_files_.begin(), glob_files_.end()); }
    globfree(&g);
#else
    ret = OB_NOT_SUPPORTED;
#endif
  } else if (OB_FAIL(ObFileSqlUtils::get_file_snapshot(o.path_, snapshot_))) {
  } else if (format_ == FILE_FORMAT_PARQUET) {
    parquet_reader_ = new (std::nothrow) ObParquetReader();
    if (OB_ISNULL(parquet_reader_)) ret = OB_ALLOCATE_MEMORY_FAILED;
    else ret = parquet_reader_->open(o.path_, projected_columns_);
  } else { stream_.open(o.path_.c_str(), std::ios::in | std::ios::binary); if (!stream_.is_open()) ret = OB_FILE_NOT_EXIST; }
  return ret;
}

int ObFileTableScanner::check_unchanged() const
{
  int ret = OB_SUCCESS;
  if (format_ != FILE_FORMAT_GLOB) {
    ObFileSnapshot now;
    if (OB_FAIL(ObFileSqlUtils::get_file_snapshot(options_.path_, now))) {}
    else if (now.size_ != snapshot_.size_ || now.mtime_ != snapshot_.mtime_
             || now.mtime_nsec_ != snapshot_.mtime_nsec_) {
      ret = OB_ERR_UNEXPECTED; LOG_WARN("file changed while scanning", K(ret), K(options_.path_.c_str()));
    }
  }
  return ret;
}

int ObFileTableScanner::read_csv_record(std::vector<std::string> &row)
{
  int ret = OB_SUCCESS; row.clear(); std::string field; bool quoted = false, after_quote = false, any = false;
  while (OB_SUCC(ret)) {
    int n = stream_.get();
    if (n == EOF) {
      if (quoted) ret = OB_INVALID_DATA; else if (!any && field.empty() && row.empty()) ret = OB_ITER_END; else row.push_back(field); break;
    }
    any = true; char c = static_cast<char>(n);
    if (quoted) {
      if (c == options_.quote_) {
        if (options_.escape_ == options_.quote_ && stream_.peek() == options_.quote_) { stream_.get(); field.push_back(c); }
        else { quoted = false; after_quote = true; }
      } else if (c == options_.escape_ && options_.escape_ != options_.quote_) {
        int e = stream_.get(); if (e == EOF) ret = OB_INVALID_DATA; else field.push_back(static_cast<char>(e));
      } else field.push_back(c);
    } else if (after_quote) {
      if (c == options_.delimiter_) { row.push_back(field); field.clear(); after_quote = false; }
      else if (c == '\n') { row.push_back(field); break; }
      else if (c == '\r') { if (stream_.peek() == '\n') stream_.get(); row.push_back(field); break; }
      else ret = OB_INVALID_DATA;
    } else if (field.empty() && c == options_.quote_) quoted = true;
    else if (c == options_.delimiter_) { row.push_back(field); field.clear(); }
    else if (c == '\n') { row.push_back(field); break; }
    else if (c == '\r') { if (stream_.peek() == '\n') stream_.get(); row.push_back(field); break; }
    else if (c == options_.quote_) ret = OB_INVALID_DATA;
    else field.push_back(c);
    if (field.size() > static_cast<size_t>(MAX_FILE_SQL_FIELD_SIZE)) ret = OB_SIZE_OVERFLOW;
  }
  if (OB_SUCC(ret)) {
    for (size_t i = 0; OB_SUCC(ret) && i < row.size(); ++i) if (!valid_utf8(row[i])) ret = OB_INVALID_DATA;
  }
  ++row_number_; return ret;
}

int ObFileTableScanner::next_csv(std::vector<std::string> &v, std::vector<bool> &nulls)
{
  int ret = read_csv_record(v);
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    LOG_WARN("invalid CSV row", K(ret), K(row_number_));
  }
  if (OB_SUCC(ret) && options_.header_ && row_number_ == 1 && !columns_.empty()) ret = read_csv_record(v);
  if (OB_SUCC(ret)) {
    nulls.resize(v.size()); for (size_t i = 0; i < v.size(); ++i) nulls[i] = v[i] == options_.null_string_;
    if (!columns_.empty() && v.size() != columns_.size()) { ret = OB_INVALID_DATA; LOG_WARN("CSV column count mismatch", K(ret), K(row_number_)); }
  }
  return ret;
}

int ObFileTableScanner::next_json(std::vector<std::string> &v, std::vector<bool> &nulls)
{
  int ret = OB_SUCCESS; std::string line;
  do { if (!std::getline(stream_, line)) return OB_ITER_END; ++row_number_; if (!line.empty() && line.back() == '\r') line.pop_back(); } while (line.empty());
  std::vector<std::pair<std::string, JsonScalar> > fields;
  if (OB_FAIL(parse_flat_object(line, fields))) { LOG_WARN("invalid JSONL row", K(ret), K(row_number_)); }
  else {
    v.assign(columns_.size(), std::string()); nulls.assign(columns_.size(), true); std::map<std::string,size_t> indexes;
    for (size_t i = 0; i < columns_.size(); ++i) indexes[columns_[i].name_] = i;
    for (size_t i = 0; OB_SUCC(ret) && i < fields.size(); ++i) {
      std::map<std::string,size_t>::const_iterator it = indexes.find(fields[i].first);
      if (it == indexes.end()) ret = OB_ERR_UNEXPECTED;
      else { v[it->second] = fields[i].second.value_; nulls[it->second] = fields[i].second.is_null_; }
    }
  }
  return ret;
}

int ObFileTableScanner::next_glob(std::vector<std::string> &v, std::vector<bool> &nulls)
{
  if (glob_pos_ >= glob_files_.size()) return OB_ITER_END;
  v.assign(1, glob_files_[glob_pos_++]); nulls.assign(1, false); return OB_SUCCESS;
}

int ObFileTableScanner::next(std::vector<std::string> &v, std::vector<bool> &nulls)
{
  int ret = check_unchanged();
  if (OB_SUCC(ret)) ret = format_ == FILE_FORMAT_CSV ? next_csv(v,nulls)
      : format_ == FILE_FORMAT_JSONL ? next_json(v,nulls)
      : format_ == FILE_FORMAT_GLOB ? next_glob(v,nulls)
      : format_ == FILE_FORMAT_PARQUET && OB_NOT_NULL(parquet_reader_) ? parquet_reader_->next(v,nulls)
      : OB_NOT_SUPPORTED;
  return ret;
}

int ObFileTableScanner::rescan()
{
  if (format_ == FILE_FORMAT_PARQUET && OB_NOT_NULL(parquet_reader_)) {
    return parquet_reader_->rescan();
  }
  ObFileTableFormat fmt = format_; ObFileTableOptions options = options_;
  std::vector<ObFileTableColumn> columns = columns_;
  std::vector<int64_t> projected_columns = projected_columns_;
  return open(fmt, options, columns, projected_columns);
}
void ObFileTableScanner::close()
{
  if (stream_.is_open()) stream_.close(); stream_.clear(); columns_.clear(); projected_columns_.clear();
  glob_files_.clear(); glob_pos_ = 0; row_number_ = 0;
  if (OB_NOT_NULL(parquet_reader_)) { delete parquet_reader_; parquet_reader_ = NULL; }
}

} } // oceanbase::sql
