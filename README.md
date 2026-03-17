# Batchflow.EntityFrameworkExtensions

Standalone repository for a future BatchFlow-focused Entity Framework bulk operations library.

## Purpose

The goal of this project is to provide an internal-first, PostgreSQL-first alternative to commercial EF bulk tooling for BatchFlow import workloads such as:

- bulk merge by configurable business keys
- snapshot synchronization with delete-missing behavior
- low-memory imports that avoid heavy EF change tracking

## Repository layout

- `src/Batchflow.EntityFrameworkExtensions` contains the library
- `tests/Batchflow.EntityFrameworkExtensions.Tests` contains unit tests

## Current status

The repository has been bootstrapped with package metadata, configuration primitives, and initial tests. Execution logic has not been implemented yet.
