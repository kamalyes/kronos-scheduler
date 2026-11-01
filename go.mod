module github.com/kamalyes/kronos-scheduler

go 1.25.0

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0
	github.com/kamalyes/go-cachex v0.5.0
	github.com/kamalyes/go-config v0.25.1
	github.com/kamalyes/go-logger v0.9.0
	github.com/kamalyes/go-sqlbuilder v0.8.0
	github.com/kamalyes/go-toolbox v0.19.2
	github.com/redis/go-redis/v9 v9.18.0
	github.com/stretchr/testify v1.12.1
	google.golang.org/genproto/googleapis/api v0.0.0-20260414002931-afd174a4e478
	google.golang.org/grpc v1.82.1
	google.golang.org/protobuf v1.36.11
	gorm.io/driver/sqlite v1.6.0
	gorm.io/gorm v1.31.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgraph-io/ristretto/v2 v2.4.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/kamalyes/go-argus v0.5.1 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mattn/go-sqlite3 v1.14.22 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// 本地开发替换
// replace github.com/kamalyes/go-toolbox => ../go-toolbox

// replace github.com/kamalyes/go-jsonpath => ../go-jsonpath

// replace github.com/kamalyes/go-logger => ../go-logger

// replace github.com/kamalyes/go-sqlbuilder => ../go-sqlbuilder

// replace github.com/kamalyes/go-config => ../go-config

// replace github.com/kamalyes/go-cachex => ../go-cachex
