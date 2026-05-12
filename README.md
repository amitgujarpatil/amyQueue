# AmyQueue - A Distributed Message Queue

> **"Let's build Amit's Kafka"** - A learning project to understand distributed systems, consensus algorithms, and message queues.

AmyQueue is a distributed, fault-tolerant message queue system inspired by Apache Kafka, built from scratch in Go. This project implements Raft consensus for metadata management, partition-based storage, consumer groups, and replication.

## 🎯 Project Goals

- **Learn by Building**: Understand distributed systems concepts by implementing them
- **Raft Consensus**: Implement Raft algorithm for controller cluster coordination
- **Kafka-like Architecture**: Partition-based storage with leader election and replication
- **Production Patterns**: Consumer groups, offset management, and fault tolerance
- **Modern Stack**: gRPC for communication, Protocol Buffers for serialization

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────┐
│           CONTROL PLANE (Controllers)               │
│         Raft Consensus Cluster (3-5 nodes)          │
│   Manages: Topics, Partitions, Brokers, Metadata    │
└─────────────────────────────────────────────────────┘
                        │
                        │ Metadata
                        ▼
┌─────────────────────────────────────────────────────┐
│             DATA PLANE (Brokers)                    │
│        Scalable Worker Nodes (unlimited)            │
│   Handles: Messages, Consumer Groups, Replication   │
└─────────────────────────────────────────────────────┘
```

### Key Components

- **Controllers**: Raft-based consensus cluster managing cluster metadata
- **Brokers**: Worker nodes storing and serving partition data
- **Producers**: Clients that publish messages to topics
- **Consumers**: Clients that subscribe to topics and consume messages
- **Consumer Groups**: Coordinated consumption with offset tracking

## 📚 Documentation

Comprehensive design documents are available in the root directory:

- [Raft Learning Guide](./raft-learning-guide.md) - Understanding Raft consensus
- [Raft with Node Roles](./raft-with-node-roles.md) - Controller/Worker architecture
- [Metadata Management Design](./metadata-management-design.md) - Topic, partition, and broker metadata
- [Worker Data Plane Design](./worker-data-plane-design.md) - Message storage and consumer groups
- [Partition Leadership and Replication](./partition-leadership-and-replication.md) - How writes and replication work
- [HTTP API Design](./http-api-design.md) - REST API for management operations
- [Protocol Comparison](./protocol-comparison-and-recommendation.md) - TCP vs gRPC vs HTTP analysis

See [docs/ROADMAP.md](./docs/ROADMAP.md) for the implementation roadmap.

## 🚀 Quick Start

### Prerequisites

- Go 1.21+
- Protocol Buffers compiler (`protoc`)
- gRPC tools

```bash
# Install protoc
brew install protobuf  # macOS
# or
apt-get install protobuf-compiler  # Linux

# Install Go plugins for protoc
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/amyqueue.git
cd amyqueue

# Install dependencies
go mod download

# Generate protobuf code
make proto

# Build all binaries
make build
```

### Running a Local Cluster

```bash
# Start a 3-node controller cluster
./scripts/start-controllers.sh

# Start broker nodes
./scripts/start-brokers.sh

# Create a topic
./bin/cli topic create --name orders --partitions 10 --replication-factor 3

# Produce messages
./bin/cli produce --topic orders --key user123 --value '{"order_id": "123"}'

# Consume messages
./bin/cli consume --topic orders --group checkout-service
```

## 🛠️ Development

### Project Structure

```
amyqueue/
├── src/
│   ├── cmd/                    # Application entry points
│   │   ├── controller/         # Controller node binary
│   │   ├── broker/             # Broker node binary
│   │   └── cli/                # CLI tool
│   ├── internal/               # Internal packages
│   │   ├── raft/               # Raft consensus implementation
│   │   ├── metadata/           # Metadata state machine
│   │   ├── partition/          # Partition log storage
│   │   ├── storage/            # Persistent storage layer
│   │   ├── grpc/               # gRPC servers
│   │   ├── http/               # HTTP API handlers
│   │   ├── consumergroup/      # Consumer group management
│   │   └── config/             # Configuration management
│   └── pkg/                    # Public packages
│       ├── protocol/           # Wire protocol definitions
│       ├── logger/             # Logging utilities
│       └── metrics/            # Metrics collection
├── proto/                      # Protocol Buffer definitions
├── scripts/                    # Utility scripts
├── test/                       # Integration tests
├── docs/                       # Additional documentation
├── .env.example                # Environment configuration template
├── Makefile                    # Build automation
└── README.md                   # This file
```

### Building

```bash
# Build all binaries
make build

# Build specific component
make build-controller
make build-broker
make build-cli

# Run tests
make test

# Run with race detector
make test-race

# Generate code coverage
make coverage
```

### Testing

```bash
# Unit tests
make test

# Integration tests
make test-integration

# End-to-end tests
make test-e2e

# Run specific test
go test ./internal/raft -v -run TestLeaderElection
```

## 📖 Usage Examples

### Creating Topics

```bash
# Create a topic with default settings
./bin/cli topic create --name orders

# Create with custom partitions and replication
./bin/cli topic create --name payments \
  --partitions 20 \
  --replication-factor 3 \
  --retention-ms 604800000
```

### Producing Messages

```bash
# Produce with key (for partitioning)
./bin/cli produce --topic orders \
  --key user123 \
  --value '{"order_id": "123", "amount": 99.99}'

# Produce from file
./bin/cli produce --topic orders --file messages.json

# Produce with headers
./bin/cli produce --topic orders \
  --key user123 \
  --value '{"order_id": "123"}' \
  --header source:checkout-service \
  --header trace-id:abc-123
```

### Consuming Messages

```bash
# Consume from beginning
./bin/cli consume --topic orders --from-beginning

# Consume with consumer group
./bin/cli consume --topic orders \
  --group checkout-service \
  --consumer-id consumer-1

# Consume specific partition
./bin/cli consume --topic orders \
  --partition 0 \
  --offset 12345
```

### Managing the Cluster

```bash
# List topics
./bin/cli topic list

# Describe topic
./bin/cli topic describe --name orders

# List brokers
./bin/cli broker list

# Check cluster health
./bin/cli cluster health
```

## 🧪 Testing

The project includes comprehensive testing at multiple levels:

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test component interactions
- **End-to-End Tests**: Test complete workflows
- **Chaos Tests**: Test fault tolerance and recovery

Run the full test suite:

```bash
make test-all
```

## 📊 Monitoring

AmyQueue exposes Prometheus metrics on port 9090 (configurable):

```bash
# View metrics
curl http://localhost:9090/metrics
```

Key metrics:
- `amyqueue_messages_produced_total` - Total messages produced
- `amyqueue_messages_consumed_total` - Total messages consumed
- `amyqueue_partition_lag` - Consumer group lag
- `amyqueue_raft_term` - Current Raft term
- `amyqueue_broker_health` - Broker health status

## 🗺️ Roadmap

### Phase 1: Foundation (Completed ✅)
- [x] Project setup and documentation
- [x] Architecture design

### Phase 2: Raft Implementation (In Progress 🚧)
- [ ] Basic Raft leader election
- [ ] Log replication
- [ ] Snapshots and log compaction
- [ ] Metadata state machine

### Phase 3: Controller Cluster (Planned 📋)
- [ ] Topic management
- [ ] Broker registration and health tracking
- [ ] Partition assignment
- [ ] Leader election (partition level)

### Phase 4: Broker Implementation (Planned 📋)
- [ ] Partition log storage
- [ ] Producer API
- [ ] Consumer API
- [ ] Replication (ISR)

### Phase 5: Consumer Groups (Planned 📋)
- [ ] Group membership management
- [ ] Partition assignment strategies
- [ ] Offset management
- [ ] Rebalancing

### Phase 6: Production Features (Planned 📋)
- [ ] TLS/SSL support
- [ ] Authentication and authorization
- [ ] Compression
- [ ] Retention policies
- [ ] Metrics and monitoring

See [docs/ROADMAP.md](./docs/ROADMAP.md) for detailed milestones.

## 🤝 Contributing

This is a learning project, but contributions are welcome! Feel free to:

- Report bugs or issues
- Suggest improvements
- Submit pull requests
- Share your learnings

## 📝 License

MIT License - see [LICENSE](./LICENSE) for details.

## 🙏 Acknowledgments

- **Apache Kafka** - Inspiration for the architecture
- **Raft Consensus Algorithm** - Diego Ongaro and John Ousterhout
- **gRPC and Protocol Buffers** - Google

## 📬 Contact

- GitHub Issues: [github.com/yourusername/amyqueue/issues](https://github.com/yourusername/amyqueue/issues)
- Email: your.email@example.com

---

**Built with ❤️ as a learning journey into distributed systems.**
