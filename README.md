# dlm
a fault-tolerant distributed lock manager for Go

![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Tests](https://github.com/yaitoo/dlm/actions/workflows/tests.yml/badge.svg)](https://github.com/yaitoo/dlm/actions/workflows/tests.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/yaitoo/dlm.svg)](https://pkg.go.dev/github.com/yaitoo/dlm)
[![Codecov](https://codecov.io/gh/yaitoo/dlm/branch/main/graph/badge.svg)](https://codecov.io/gh/yaitoo/dlm)
[![GitHub Release](https://img.shields.io/github/v/release/yaitoo/dlm)](https://github.com/yaitoo/dlm/blob/main/CHANGELOG.md)
[![Go Report Card](https://goreportcard.com/badge/yaitoo/dlm)](http://goreportcard.com/report/yaitoo/dlm)


inspired by 
- https://redis.io/docs/manual/patterns/distributed-locks
- https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
- http://thesecretlivesofdata.com/raft/
- https://medium.com/nerd-for-tech/leases-fences-distributed-design-patterns-c1983eccc9b1
- https://www.golang.dk/articles/benchmarking-sqlite-performance-in-go