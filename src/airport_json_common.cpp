#include "airport_json_common.hpp"

#include "duckdb/common/exception/binder_exception.hpp"

namespace duckdb {

using AirportJSONPathType = AirportJSONCommon::JSONPathType;

string AirportJSONCommon::ValToString(yyjson_val *val, idx_t max_len) {
	AirportJSONAllocator json_allocator(Allocator::DefaultAllocator());
	idx_t len;
	auto data = AirportJSONCommon::WriteVal<yyjson_val>(val, json_allocator.GetYYAlc(), len);
	if (max_len < len) {
		return string(data, max_len) + "...";
	} else {
		return string(data, len);
	}
}

void AirportJSONCommon::ThrowValFormatError(string error_string, yyjson_val *val) {
	error_string = StringUtil::Format(error_string, AirportJSONCommon::ValToString(val));
	throw InvalidInputException(error_string);
}

string ThrowPathError(const char *ptr, const char *end, const bool binder) {
	ptr--;
	auto msg = StringUtil::Format("JSON path error near '%s'", string(ptr, end - ptr));
	if (binder) {
		throw BinderException(msg);
	} else {
		throw InvalidInputException(msg);
	}
}

struct JSONKeyReadResult {
public:
	static inline JSONKeyReadResult Empty() {
		return {idx_t(0), string()};
	}

	static inline JSONKeyReadResult WildCard() {
		return {1, "*"};
	}

	inline bool IsValid() {
		return chars_read != 0;
	}

	inline bool IsWildCard() {
		return key == "*";
	}

public:
	idx_t chars_read;
	string key;
};

static inline JSONKeyReadResult ReadString(const char *ptr, const char *const end, const bool escaped) {
	const char *const before = ptr;
	if (escaped) {
		auto key = make_unsafe_uniq_array<char>(end - ptr);
		idx_t key_len = 0;

		bool backslash = false;
		while (ptr != end) {
			if (backslash) {
				if (*ptr != '"' && *ptr != '\\') {
					key[key_len++] = '\\';
				}
				backslash = false;
			} else {
				if (*ptr == '"') {
					break;
				} else if (*ptr == '\\') {
					backslash = true;
					ptr++;
					continue;
				}
			}
			key[key_len++] = *ptr++;
		}
		if (ptr == end || backslash) {
			return JSONKeyReadResult::Empty();
		} else {
			return {idx_t(ptr - before), string(key.get(), key_len)};
		}
	} else {
		while (ptr != end) {
			if (*ptr == '.' || *ptr == '[') {
				break;
			}
			ptr++;
		}
		return {idx_t(ptr - before), string(before, ptr - before)};
	}
}

static inline idx_t ReadInteger(const char *ptr, const char *const end, idx_t &idx) {
	static constexpr auto IDX_T_SAFE_DIG = 19;
	static constexpr auto IDX_T_MAX = ((idx_t)(~(idx_t)0));

	const char *const before = ptr;
	idx = 0;
	for (idx_t i = 0; i < IDX_T_SAFE_DIG; i++) {
		if (ptr == end) {
			// No closing ']'
			return 0;
		}
		if (*ptr == ']') {
			break;
		}
		uint8_t add = (uint8_t)(*ptr - '0');
		if (add <= 9) {
			idx = add + idx * 10;
		} else {
			// Not a digit
			return 0;
		}
		ptr++;
	}
	// Invalid if overflow
	return idx >= (idx_t)IDX_T_MAX ? 0 : ptr - before;
}

static inline JSONKeyReadResult ReadKey(const char *ptr, const char *const end) {
	D_ASSERT(ptr != end);
	if (*ptr == '*') { // Wildcard
		return JSONKeyReadResult::WildCard();
	}
	bool escaped = false;
	if (*ptr == '"') {
		ptr++; // Skip past opening '"'
		escaped = true;
	}
	auto result = ReadString(ptr, end, escaped);
	if (!result.IsValid()) {
		return result;
	}
	if (escaped) {
		result.chars_read += 2; // Account for surrounding quotes
	}
	return result;
}

static inline bool ReadArrayIndex(const char *&ptr, const char *const end, idx_t &array_index, bool &from_back) {
	D_ASSERT(ptr != end);
	from_back = false;
	if (*ptr == '*') { // Wildcard
		ptr++;
		if (ptr == end || *ptr != ']') {
			return false;
		}
		array_index = DConstants::INVALID_INDEX;
	} else {
		if (*ptr == '#') { // SQLite syntax to index from back of array
			ptr++;         // Skip over '#'
			if (ptr == end) {
				return false;
			}
			if (*ptr == ']') {
				// [#] always returns NULL in SQLite, so we return an array index that will do the same
				array_index = NumericLimits<uint32_t>::Maximum();
				ptr++;
				return true;
			}
			if (*ptr != '-') {
				return false;
			}
			from_back = true;
		}
		if (*ptr == '-') {
			ptr++; // Skip over '-'
			from_back = true;
		}
		auto idx_len = ReadInteger(ptr, end, array_index);
		if (idx_len == 0) {
			return false;
		}
		ptr += idx_len;
	}
	ptr++; // Skip past closing ']'
	return true;
}

AirportJSONPathType AirportJSONCommon::ValidatePath(const char *ptr, const idx_t &len, const bool binder) {
	D_ASSERT(len >= 1 && *ptr == '$');
	AirportJSONPathType path_type = JSONPathType::REGULAR;
	const char *const end = ptr + len;
	ptr++; // Skip past '$'
	while (ptr != end) {
		const auto &c = *ptr++;
		if (ptr == end) {
			ThrowPathError(ptr, end, binder);
		}
		switch (c) {
		case '.': { // Object field
			auto key = ReadKey(ptr, end);
			if (!key.IsValid()) {
				ThrowPathError(ptr, end, binder);
			} else if (key.IsWildCard()) {
				path_type = JSONPathType::WILDCARD;
			}
			ptr += key.chars_read;
			break;
		}
		case '[': { // Array index
			idx_t array_index;
			bool from_back;
			if (!ReadArrayIndex(ptr, end, array_index, from_back)) {
				ThrowPathError(ptr, end, binder);
			}
			if (array_index == DConstants::INVALID_INDEX) {
				path_type = JSONPathType::WILDCARD;
			}
			break;
		}
		default:
			ThrowPathError(ptr, end, binder);
		}
	}
	return path_type;
}

yyjson_val *AirportJSONCommon::GetPath(yyjson_val *val, const char *ptr, const idx_t &len) {
	// Path has been validated at this point
	const char *const end = ptr + len;
	ptr++; // Skip past '$'
	while (val != nullptr && ptr != end) {
		const auto &c = *ptr++;
		D_ASSERT(ptr != end);
		switch (c) {
		case '.': { // Object field
			if (!unsafe_yyjson_is_obj(val)) {
				return nullptr;
			}
			auto key_result = ReadKey(ptr, end);
			D_ASSERT(key_result.IsValid());
			ptr += key_result.chars_read;
			val = yyjson_obj_getn(val, key_result.key.c_str(), key_result.key.size());
			break;
		}
		case '[': { // Array index
			if (!unsafe_yyjson_is_arr(val)) {
				return nullptr;
			}
			idx_t array_index;
			bool from_back;
#ifdef DEBUG
			bool success =
#endif
			    ReadArrayIndex(ptr, end, array_index, from_back);
#ifdef DEBUG
			D_ASSERT(success);
#endif
			if (from_back && array_index != 0) {
				array_index = unsafe_yyjson_get_len(val) - array_index;
			}
			val = yyjson_arr_get(val, array_index);
			break;
		}
		default: // LCOV_EXCL_START
			throw InternalException(
			    "Invalid JSON Path encountered in JSONCommon::GetPath, call JSONCommon::ValidatePath first!");
		} // LCOV_EXCL_STOP
	}
	return val;
}

void GetWildcardPathInternal(yyjson_val *val, const char *ptr, const char *const end, vector<yyjson_val *> &vals) {
	while (val != nullptr && ptr != end) {
		const auto &c = *ptr++;
		D_ASSERT(ptr != end);
		switch (c) {
		case '.': { // Object field
			if (!unsafe_yyjson_is_obj(val)) {
				return;
			}
			auto key_result = ReadKey(ptr, end);
			D_ASSERT(key_result.IsValid());
			ptr += key_result.chars_read;
			if (key_result.IsWildCard()) { // Wildcard
				size_t idx, max;
				yyjson_val *key, *obj_val;
				yyjson_obj_foreach(val, idx, max, key, obj_val) {
					GetWildcardPathInternal(obj_val, ptr, end, vals);
				}
				return;
			}
			val = yyjson_obj_getn(val, key_result.key.c_str(), key_result.key.size());
			break;
		}
		case '[': { // Array index
			if (!unsafe_yyjson_is_arr(val)) {
				return;
			}
			idx_t array_index;
			bool from_back;
#ifdef DEBUG
			bool success =
#endif
			    ReadArrayIndex(ptr, end, array_index, from_back);
#ifdef DEBUG
			D_ASSERT(success);
#endif

			if (array_index == DConstants::INVALID_INDEX) { // Wildcard
				size_t idx, max;
				yyjson_val *arr_val;
				yyjson_arr_foreach(val, idx, max, arr_val) {
					GetWildcardPathInternal(arr_val, ptr, end, vals);
				}
				return;
			}
			if (from_back && array_index != 0) {
				array_index = unsafe_yyjson_get_len(val) - array_index;
			}
			val = yyjson_arr_get(val, array_index);
			break;
		}
		default: // LCOV_EXCL_START
			throw InternalException(
			    "Invalid JSON Path encountered in GetWildcardPathInternal, call JSONCommon::ValidatePath first!");
		} // LCOV_EXCL_STOP
	}
	if (val != nullptr) {
		vals.emplace_back(val);
	}
}

void AirportJSONCommon::GetWildcardPath(yyjson_val *val, const char *ptr, const idx_t &len, vector<yyjson_val *> &vals) {
	// Path has been validated at this point
	const char *const end = ptr + len;
	ptr++; // Skip past '$'
	GetWildcardPathInternal(val, ptr, end, vals);
}

} // namespace duckdb
