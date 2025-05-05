#pragma once

#include "duckdb.hpp"
#include <limits>
#include <cstdint>
#include <numeric>

#ifdef _WIN32

// Windows defines min and max macros that mess up std::min/max
#ifndef NOMINMAX
#define NOMINMAX
#endif

// #define WIN32_LEAN_AND_MEAN

// Set Windows 7 as a conservative minimum for Apache Arrow
// #if defined(_WIN32_WINNT) && _WIN32_WINNT < 0x601
// #undef _WIN32_WINNT
// #endif
// #ifndef _WIN32_WINNT
// #define _WIN32_WINNT 0x601
// #endif

#ifdef max
#undef max
#endif
#ifdef min
#undef min
#endif

// // The Windows API defines macros from *File resolving to either
// // *FileA or *FileW.  Need to undo them.
// #ifdef CopyFile
// #undef CopyFile
// #endif
// #ifdef CreateFile
// #undef CreateFile
// #endif
// #ifdef DeleteFile
// #undef DeleteFile
// #endif

// // Other annoying Windows macro definitions...
// #ifdef IN
// #undef IN
// #endif
// #ifdef OUT
// #undef OUT
// #endif

// Note that we can't undefine OPTIONAL, because it can be used in other
// Windows headers...

#endif // _WIN32

namespace duckdb
{

	class AirportExtension : public Extension
	{
	public:
		void Load(DuckDB &db) override;
		std::string Name() override;
		std::string Version() const override;
	};

	void AirportAddListFlightsFunction(DatabaseInstance &instance);
	void AirportAddTakeFlightFunction(DatabaseInstance &instance);

} // namespace duckdb
