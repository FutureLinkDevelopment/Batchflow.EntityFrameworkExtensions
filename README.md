# Batchflow.EntityFrameworkExtensions

Standalone repository for a BatchFlow-focused Entity Framework bulk operations library.

## Purpose

The goal of this project is to provide an internal-first, PostgreSQL-first alternative to commercial EF bulk tooling for BatchFlow import workloads such as:

- bulk merge by configurable business keys
- snapshot synchronization with delete-missing behavior
- bulk insert and delete-by-key operations
- low-memory imports that avoid heavy EF change tracking

## Repository layout

- `src/Batchflow.EntityFrameworkExtensions` contains the library
- `tests/Batchflow.EntityFrameworkExtensions.Tests` contains unit tests
- `benchmarks/Batchflow.EntityFrameworkExtensions.Benchmarks` contains BenchmarkDotNet performance benchmarks

## Current status

The library currently supports SQLite and PostgreSQL-style bulk insert, merge/upsert, synchronize, and delete-by-key operations. The current focus is expanding coverage, tightening edge-case behavior, and improving large-batch performance.

## Benchmarks

The repository includes a standalone benchmark project for measuring the current tracked EF-style upsert and synchronization baselines alongside the package bulk implementations.

Run it with:

```bash
dotnet run -c Release --project benchmarks/Batchflow.EntityFrameworkExtensions.Benchmarks/Batchflow.EntityFrameworkExtensions.Benchmarks.csproj
```

The benchmark project is a separate executable and is not included when packing `src/Batchflow.EntityFrameworkExtensions/Batchflow.EntityFrameworkExtensions.csproj`.
