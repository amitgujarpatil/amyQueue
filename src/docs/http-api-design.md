# HTTP REST API Design for Metadata Management

## Overview

This document describes the HTTP/REST API layer for managing topics, partitions, brokers, and cluster metadata. The HTTP API wraps the same underlying services as gRPC, providing an easy-to-use interface for developers and tools.

---

## Architecture: HTTP + gRPC Dual Interface

```
┌─────────────────────────────────────────────────────────┐
│                    CLIENT LAYER                         │
│                                                          │
│  ┌──────────────┐              ┌──────────────┐        │
│  │ HTTP Clients │              │ gRPC Clients │        │
│  │ (curl, web)  │              │ (SDKs, tools)│        │
│  └──────┬───────┘              └──────┬───────┘        │
└─────────┼──────────────────────────────┼────────────────┘
          │                              │
          │ REST                         │ gRPC
          ▼                              ▼
┌─────────────────────────────────────────────────────────┐
│                   API LAYER                             │
│                                                          │
│  ┌──────────────┐              ┌──────────────┐        │
│  │ HTTP Handler │              │ gRPC Server  │        │
│  └──────┬───────┘              └──────┬───────┘        │
│         │                              │                │
│         └──────────┬───────────────────┘                │
│                    ▼                                     │
│         ┌─────────────────────┐                         │
│         │  Metadata Service   │                         │
│         │  (Business Logic)   │                         │
│         └──────────┬──────────┘                         │
└────────────────────┼────────────────────────────────────┘
                     │
                     ▼
          ┌──────────────────┐
          │  Raft Consensus  │
          └──────────────────┘
```

**Key Design:**
- Both HTTP and gRPC share the same service layer
- HTTP handlers translate REST requests → Service calls
- gRPC handlers translate proto messages → Service calls
- Single source of truth for business logic

---

## HTTP API Endpoints

### Base URL Structure

```
Controllers: http://localhost:8080/api/v1/...
Workers:     http://localhost:9090/api/v1/...
```

### Topic Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/topics` | Create a new topic |
| GET | `/api/v1/topics` | List all topics |
| GET | `/api/v1/topics/{name}` | Get topic details |
| PUT | `/api/v1/topics/{name}` | Update topic config |
| DELETE | `/api/v1/topics/{name}` | Delete a topic |
| GET | `/api/v1/topics/{name}/partitions` | List topic partitions |
| GET | `/api/v1/topics/{name}/partitions/{id}` | Get partition details |

### Broker Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/brokers` | Register a broker |
| GET | `/api/v1/brokers` | List all brokers |
| GET | `/api/v1/brokers/{id}` | Get broker details |
| DELETE | `/api/v1/brokers/{id}` | Deregister a broker |
| POST | `/api/v1/brokers/{id}/heartbeat` | Send heartbeat |

### Partition Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/partitions/reassign` | Reassign partitions |
| PUT | `/api/v1/partitions/{topic}/{id}/leader` | Change partition leader |
| GET | `/api/v1/partitions/under-replicated` | List under-replicated partitions |

### Cluster Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/cluster/info` | Get cluster information |
| GET | `/api/v1/cluster/health` | Cluster health check |
| GET | `/api/v1/cluster/metadata` | Get full cluster metadata |
| PUT | `/api/v1/cluster/config` | Update cluster config |

### Raft/Controller Status

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/raft/status` | Get Raft node status |
| GET | `/api/v1/raft/leader` | Get current leader info |
| GET | `/api/v1/raft/peers` | List controller peers |

---

## API Request/Response Examples

### 1. Create Topic

**Request:**
```http
POST /api/v1/topics HTTP/1.1
Host: localhost:8080
Content-Type: application/json

{
  "name": "orders",
  "num_partitions": 10,
  "replication_factor": 3,
  "config": {
    "retention.ms": "604800000",
    "retention.bytes": "1073741824",
    "segment.ms": "86400000",
    "cleanup.policy": "delete",
    "compression.type": "snappy",
    "max.message.bytes": "1048576",
    "min.insync.replicas": "2"
  }
}
```

**Response (Success):**
```http
HTTP/1.1 201 Created
Content-Type: application/json

{
  "success": true,
  "topic_id": "topic-550e8400-e29b-41d4-a716-446655440000",
  "name": "orders",
  "num_partitions": 10,
  "replication_factor": 3,
  "created_at": "2026-05-12T10:00:00Z"
}
```

**Response (Error - Not Leader):**
```http
HTTP/1.1 307 Temporary Redirect
Content-Type: application/json
Location: http://controller-2:8080/api/v1/topics

{
  "success": false,
  "error": "not_leader",
  "message": "This node is not the leader",
  "leader": "controller-2",
  "leader_address": "http://controller-2:8080"
}
```

**Response (Error - Validation):**
```http
HTTP/1.1 400 Bad Request
Content-Type: application/json

{
  "success": false,
  "error": "validation_error",
  "message": "num_partitions must be positive",
  "field": "num_partitions"
}
```

### 2. List Topics

**Request:**
```http
GET /api/v1/topics HTTP/1.1
Host: localhost:8080
```

**Response:**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "topics": [
    {
      "topic_id": "topic-550e8400-e29b-41d4-a716-446655440000",
      "name": "orders",
      "num_partitions": 10,
      "replication_factor": 3,
      "created_at": "2026-05-12T10:00:00Z"
    },
    {
      "topic_id": "topic-660e8400-e29b-41d4-a716-446655440111",
      "name": "payments",
      "num_partitions": 5,
      "replication_factor": 3,
      "created_at": "2026-05-12T10:05:00Z"
    }
  ],
  "total": 2
}
```

### 3. Get Topic Details

**Request:**
```http
GET /api/v1/topics/orders HTTP/1.1
Host: localhost:8080
```

**Response:**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "topic_id": "topic-550e8400-e29b-41d4-a716-446655440000",
  "name": "orders",
  "num_partitions": 10,
  "replication_factor": 3,
  "config": {
    "retention.ms": "604800000",
    "retention.bytes": "1073741824",
    "segment.ms": "86400000",
    "cleanup.policy": "delete",
    "compression.type": "snappy",
    "max.message.bytes": "1048576",
    "min.insync.replicas": "2"
  },
  "partitions": [
    {
      "partition_id": 0,
      "leader": "broker-1",
      "replicas": ["broker-1", "broker-2", "broker-3"],
      "isr": ["broker-1", "broker-2"],
      "offline_replicas": [],
      "leader_epoch": 5
    },
    {
      "partition_id": 1,
      "leader": "broker-2",
      "replicas": ["broker-2", "broker-3", "broker-4"],
      "isr": ["broker-2", "broker-3", "broker-4"],
      "offline_replicas": [],
      "leader_epoch": 3
    }
  ],
  "created_at": "2026-05-12T10:00:00Z",
  "updated_at": "2026-05-12T10:15:00Z"
}
```

### 4. Update Topic Config

**Request:**
```http
PUT /api/v1/topics/orders HTTP/1.1
Host: localhost:8080
Content-Type: application/json

{
  "config": {
    "retention.ms": "1209600000",
    "min.insync.replicas": "3"
  }
}
```

**Response:**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "success": true,
  "message": "Topic config updated",
  "updated_fields": ["retention.ms", "min.insync.replicas"]
}
```

### 5. Delete Topic

**Request:**
```http
DELETE /api/v1/topics/orders HTTP/1.1
Host: localhost:8080
```

**Response:**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "success": true,
  "message": "Topic 'orders' deleted"
}
```

### 6. Register Broker

**Request:**
```http
POST /api/v1/brokers HTTP/1.1
Host: localhost:8080
Content-Type: application/json

{
  "broker_id": "broker-5",
  "host": "192.168.1.105",
  "port": 9092,
  "rack": "rack-2",
  "endpoints": {
    "plaintext": "192.168.1.105:9092",
    "grpc": "192.168.1.105:9093"
  }
}
```

**Response:**
```http
HTTP/1.1 201 Created
Content-Type: application/json

{
  "success": true,
  "broker_id": "broker-5",
  "registered_at": "2026-05-12T11:00:00Z"
}
```

### 7. List Brokers

**Request:**
```http
GET /api/v1/brokers HTTP/1.1
Host: localhost:8080
```

**Response:**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "brokers": [
    {
      "broker_id": "broker-1",
      "host": "192.168.1.101",
      "port": 9092,
      "status": "alive",
      "last_heartbeat": "2026-05-12T11:05:30Z",
      "registered_at": "2026-05-12T10:00:00Z",
      "resources": {
        "disk_total_bytes": 1099511627776,
        "disk_used_bytes": 549755813888,
        "cpu_usage_percent": 45.5
      }
    },
    {
      "broker_id": "broker-2",
      "host": "192.168.1.102",
      "port": 9092,
      "status": "alive",
      "last_heartbeat": "2026-05-12T11:05:31Z",
      "registered_at": "2026-05-12T10:00:00Z",
      "resources": {
        "disk_total_bytes": 1099511627776,
        "disk_used_bytes": 439804651110,
        "cpu_usage_percent": 38.2
      }
    }
  ],
  "total": 2
}
```

### 8. Send Heartbeat

**Request:**
```http
POST /api/v1/brokers/broker-1/heartbeat HTTP/1.1
Host: localhost:8080
Content-Type: application/json

{
  "resources": {
    "disk_total_bytes": 1099511627776,
    "disk_used_bytes": 549755813888,
    "cpu_usage_percent": 45.5,
    "network_throughput_mbps": 125.3
  }
}
```

**Response:**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "success": true,
  "metadata_version": 1523,
  "actions": []
}
```

### 9. Get Cluster Info

**Request:**
```http
GET /api/v1/cluster/info HTTP/1.1
Host: localhost:8080
```

**Response:**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "cluster_id": "cluster-abc123",
  "controller_leader": "controller-1",
  "controller_leader_address": "http://controller-1:8080",
  "num_controllers": 3,
  "num_brokers": 10,
  "num_topics": 25,
  "total_partitions": 250,
  "metadata_version": 1523,
  "raft_term": 5,
  "raft_commit_index": 1523
}
```

### 10. Get Raft Status

**Request:**
```http
GET /api/v1/raft/status HTTP/1.1
Host: localhost:8080
```

**Response:**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "node_id": "controller-1",
  "role": "controller",
  "state": "leader",
  "term": 5,
  "voted_for": "controller-1",
  "commit_index": 1523,
  "last_applied": 1523,
  "last_log_index": 1523,
  "last_log_term": 5,
  "leader_id": "controller-1",
  "peers": [
    {
      "id": "controller-2",
      "address": "http://controller-2:8080",
      "state": "follower",
      "next_index": 1524,
      "match_index": 1523
    },
    {
      "id": "controller-3",
      "address": "http://controller-3:8080",
      "state": "follower",
      "next_index": 1524,
      "match_index": 1523
    }
  ]
}
```

### 11. Get Full Metadata (for Workers)

**Request:**
```http
GET /api/v1/cluster/metadata?version=1500 HTTP/1.1
Host: localhost:8080
```

**Response:**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "version": 1523,
  "topics": [
    {
      "name": "orders",
      "partitions": [
        {
          "partition_id": 0,
          "leader": "broker-1",
          "replicas": ["broker-1", "broker-2", "broker-3"],
          "isr": ["broker-1", "broker-2"]
        }
      ]
    }
  ],
  "brokers": [
    {
      "broker_id": "broker-1",
      "host": "192.168.1.101",
      "port": 9092,
      "status": "alive",
      "endpoints": {
        "plaintext": "192.168.1.101:9092",
        "grpc": "192.168.1.101:9093"
      }
    }
  ],
  "config": {
    "cluster_id": "cluster-abc123",
    "default_replication_factor": 3,
    "min_insync_replicas": 2,
    "default_retention_ms": 604800000
  }
}
```

---

## HTTP Handler Implementation

### Main HTTP Server Setup

```go
package httpapi

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "time"
    
    "github.com/gorilla/mux"
)

type HTTPServer struct {
    router          *mux.Router
    metadataService *MetadataService
    raftNode        *RaftNode
    port            int
}

func NewHTTPServer(metadataService *MetadataService, raftNode *RaftNode, port int) *HTTPServer {
    s := &HTTPServer{
        router:          mux.NewRouter(),
        metadataService: metadataService,
        raftNode:        raftNode,
        port:            port,
    }
    
    s.setupRoutes()
    return s
}

func (s *HTTPServer) setupRoutes() {
    api := s.router.PathPrefix("/api/v1").Subrouter()
    
    // Middleware
    api.Use(s.loggingMiddleware)
    api.Use(s.corsMiddleware)
    api.Use(s.leaderCheckMiddleware) // Redirect to leader if needed
    
    // Topic routes
    api.HandleFunc("/topics", s.handleCreateTopic).Methods("POST")
    api.HandleFunc("/topics", s.handleListTopics).Methods("GET")
    api.HandleFunc("/topics/{name}", s.handleGetTopic).Methods("GET")
    api.HandleFunc("/topics/{name}", s.handleUpdateTopic).Methods("PUT")
    api.HandleFunc("/topics/{name}", s.handleDeleteTopic).Methods("DELETE")
    api.HandleFunc("/topics/{name}/partitions", s.handleListPartitions).Methods("GET")
    api.HandleFunc("/topics/{name}/partitions/{id}", s.handleGetPartition).Methods("GET")
    
    // Broker routes
    api.HandleFunc("/brokers", s.handleRegisterBroker).Methods("POST")
    api.HandleFunc("/brokers", s.handleListBrokers).Methods("GET")
    api.HandleFunc("/brokers/{id}", s.handleGetBroker).Methods("GET")
    api.HandleFunc("/brokers/{id}", s.handleDeregisterBroker).Methods("DELETE")
    api.HandleFunc("/brokers/{id}/heartbeat", s.handleHeartbeat).Methods("POST")
    
    // Cluster routes
    api.HandleFunc("/cluster/info", s.handleGetClusterInfo).Methods("GET")
    api.HandleFunc("/cluster/health", s.handleGetClusterHealth).Methods("GET")
    api.HandleFunc("/cluster/metadata", s.handleGetMetadata).Methods("GET")
    api.HandleFunc("/cluster/config", s.handleUpdateClusterConfig).Methods("PUT")
    
    // Raft routes
    api.HandleFunc("/raft/status", s.handleGetRaftStatus).Methods("GET")
    api.HandleFunc("/raft/leader", s.handleGetLeader).Methods("GET")
    api.HandleFunc("/raft/peers", s.handleGetPeers).Methods("GET")
    
    // Health check (no auth required)
    s.router.HandleFunc("/health", s.handleHealthCheck).Methods("GET")
}

func (s *HTTPServer) Start() error {
    addr := fmt.Sprintf(":%d", s.port)
    log.Printf("HTTP server starting on %s", addr)
    
    server := &http.Server{
        Addr:         addr,
        Handler:      s.router,
        ReadTimeout:  10 * time.Second,
        WriteTimeout: 10 * time.Second,
        IdleTimeout:  60 * time.Second,
    }
    
    return server.ListenAndServe()
}
```

### Middleware

```go
// Logging middleware
func (s *HTTPServer) loggingMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        
        // Wrap response writer to capture status code
        wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
        
        next.ServeHTTP(wrapped, r)
        
        log.Printf("%s %s %d %s", r.Method, r.URL.Path, wrapped.statusCode, time.Since(start))
    })
}

type responseWriter struct {
    http.ResponseWriter
    statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
    rw.statusCode = code
    rw.ResponseWriter.WriteHeader(code)
}

// CORS middleware
func (s *HTTPServer) corsMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Access-Control-Allow-Origin", "*")
        w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
        
        if r.Method == "OPTIONS" {
            w.WriteHeader(http.StatusOK)
            return
        }
        
        next.ServeHTTP(w, r)
    })
}

// Leader check middleware - redirect to leader if not leader
func (s *HTTPServer) leaderCheckMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Only check for write operations
        if r.Method == "POST" || r.Method == "PUT" || r.Method == "DELETE" {
            if !s.raftNode.IsLeader() {
                leader := s.raftNode.GetLeader()
                if leader != "" {
                    leaderURL := fmt.Sprintf("http://%s%s", leader, r.URL.Path)
                    
                    s.respondJSON(w, http.StatusTemporaryRedirect, map[string]interface{}{
                        "success":        false,
                        "error":          "not_leader",
                        "message":        "This node is not the leader",
                        "leader":         leader,
                        "leader_address": leaderURL,
                    })
                    return
                }
                
                s.respondError(w, http.StatusServiceUnavailable, "no_leader", "No leader elected")
                return
            }
        }
        
        next.ServeHTTP(w, r)
    })
}
```

### Topic Handlers

```go
// Create Topic
func (s *HTTPServer) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
    var req struct {
        Name              string            `json:"name"`
        NumPartitions     int               `json:"num_partitions"`
        ReplicationFactor int               `json:"replication_factor"`
        Config            map[string]string `json:"config"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        s.respondError(w, http.StatusBadRequest, "invalid_json", err.Error())
        return
    }
    
    // Validate
    if req.Name == "" {
        s.respondError(w, http.StatusBadRequest, "validation_error", "name is required")
        return
    }
    if req.NumPartitions <= 0 {
        s.respondError(w, http.StatusBadRequest, "validation_error", "num_partitions must be positive")
        return
    }
    if req.ReplicationFactor <= 0 {
        s.respondError(w, http.StatusBadRequest, "validation_error", "replication_factor must be positive")
        return
    }
    
    // Create topic via service
    topic, err := s.metadataService.CreateTopic(r.Context(), &CreateTopicRequest{
        Name:              req.Name,
        NumPartitions:     req.NumPartitions,
        ReplicationFactor: req.ReplicationFactor,
        Config:            req.Config,
    })
    
    if err != nil {
        s.respondError(w, http.StatusInternalServerError, "creation_failed", err.Error())
        return
    }
    
    s.respondJSON(w, http.StatusCreated, map[string]interface{}{
        "success":            true,
        "topic_id":           topic.ID,
        "name":               topic.Name,
        "num_partitions":     topic.NumPartitions,
        "replication_factor": topic.ReplicationFactor,
        "created_at":         topic.CreatedAt,
    })
}

// List Topics
func (s *HTTPServer) handleListTopics(w http.ResponseWriter, r *http.Request) {
    topics, err := s.metadataService.ListTopics(r.Context())
    if err != nil {
        s.respondError(w, http.StatusInternalServerError, "list_failed", err.Error())
        return
    }
    
    // Convert to simplified response
    topicList := make([]map[string]interface{}, len(topics))
    for i, topic := range topics {
        topicList[i] = map[string]interface{}{
            "topic_id":           topic.ID,
            "name":               topic.Name,
            "num_partitions":     topic.NumPartitions,
            "replication_factor": topic.ReplicationFactor,
            "created_at":         topic.CreatedAt,
        }
    }
    
    s.respondJSON(w, http.StatusOK, map[string]interface{}{
        "topics": topicList,
        "total":  len(topicList),
    })
}

// Get Topic
func (s *HTTPServer) handleGetTopic(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    name := vars["name"]
    
    topic, err := s.metadataService.GetTopic(r.Context(), name)
    if err != nil {
        if err == ErrTopicNotFound {
            s.respondError(w, http.StatusNotFound, "topic_not_found", fmt.Sprintf("Topic '%s' not found", name))
            return
        }
        s.respondError(w, http.StatusInternalServerError, "get_failed", err.Error())
        return
    }
    
    s.respondJSON(w, http.StatusOK, topic)
}

// Update Topic
func (s *HTTPServer) handleUpdateTopic(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    name := vars["name"]
    
    var req struct {
        Config map[string]string `json:"config"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        s.respondError(w, http.StatusBadRequest, "invalid_json", err.Error())
        return
    }
    
    err := s.metadataService.UpdateTopicConfig(r.Context(), name, req.Config)
    if err != nil {
        if err == ErrTopicNotFound {
            s.respondError(w, http.StatusNotFound, "topic_not_found", fmt.Sprintf("Topic '%s' not found", name))
            return
        }
        s.respondError(w, http.StatusInternalServerError, "update_failed", err.Error())
        return
    }
    
    // Get updated fields
    updatedFields := make([]string, 0, len(req.Config))
    for key := range req.Config {
        updatedFields = append(updatedFields, key)
    }
    
    s.respondJSON(w, http.StatusOK, map[string]interface{}{
        "success":        true,
        "message":        "Topic config updated",
        "updated_fields": updatedFields,
    })
}

// Delete Topic
func (s *HTTPServer) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    name := vars["name"]
    
    err := s.metadataService.DeleteTopic(r.Context(), name)
    if err != nil {
        if err == ErrTopicNotFound {
            s.respondError(w, http.StatusNotFound, "topic_not_found", fmt.Sprintf("Topic '%s' not found", name))
            return
        }
        s.respondError(w, http.StatusInternalServerError, "delete_failed", err.Error())
        return
    }
    
    s.respondJSON(w, http.StatusOK, map[string]interface{}{
        "success": true,
        "message": fmt.Sprintf("Topic '%s' deleted", name),
    })
}
```

### Broker Handlers

```go
// Register Broker
func (s *HTTPServer) handleRegisterBroker(w http.ResponseWriter, r *http.Request) {
    var req struct {
        BrokerID  string            `json:"broker_id"`
        Host      string            `json:"host"`
        Port      int               `json:"port"`
        Rack      string            `json:"rack"`
        Endpoints map[string]string `json:"endpoints"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        s.respondError(w, http.StatusBadRequest, "invalid_json", err.Error())
        return
    }
    
    err := s.metadataService.RegisterBroker(r.Context(), &RegisterBrokerRequest{
        BrokerID:  req.BrokerID,
        Host:      req.Host,
        Port:      req.Port,
        Rack:      req.Rack,
        Endpoints: req.Endpoints,
    })
    
    if err != nil {
        s.respondError(w, http.StatusInternalServerError, "registration_failed", err.Error())
        return
    }
    
    s.respondJSON(w, http.StatusCreated, map[string]interface{}{
        "success":       true,
        "broker_id":     req.BrokerID,
        "registered_at": time.Now(),
    })
}

// List Brokers
func (s *HTTPServer) handleListBrokers(w http.ResponseWriter, r *http.Request) {
    brokers, err := s.metadataService.ListBrokers(r.Context())
    if err != nil {
        s.respondError(w, http.StatusInternalServerError, "list_failed", err.Error())
        return
    }
    
    s.respondJSON(w, http.StatusOK, map[string]interface{}{
        "brokers": brokers,
        "total":   len(brokers),
    })
}

// Heartbeat
func (s *HTTPServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    brokerID := vars["id"]
    
    var req struct {
        Resources struct {
            DiskTotalBytes       int64   `json:"disk_total_bytes"`
            DiskUsedBytes        int64   `json:"disk_used_bytes"`
            CPUUsagePercent      float64 `json:"cpu_usage_percent"`
            NetworkThroughputMbps float64 `json:"network_throughput_mbps"`
        } `json:"resources"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        s.respondError(w, http.StatusBadRequest, "invalid_json", err.Error())
        return
    }
    
    metadataVersion, err := s.metadataService.ProcessHeartbeat(r.Context(), brokerID, &req.Resources)
    if err != nil {
        s.respondError(w, http.StatusInternalServerError, "heartbeat_failed", err.Error())
        return
    }
    
    s.respondJSON(w, http.StatusOK, map[string]interface{}{
        "success":          true,
        "metadata_version": metadataVersion,
        "actions":          []string{}, // Future: return actions to take
    })
}
```

### Cluster Handlers

```go
// Get Cluster Info
func (s *HTTPServer) handleGetClusterInfo(w http.ResponseWriter, r *http.Request) {
    info := s.metadataService.GetClusterInfo(r.Context())
    
    s.respondJSON(w, http.StatusOK, map[string]interface{}{
        "cluster_id":                info.ClusterID,
        "controller_leader":         s.raftNode.GetLeader(),
        "controller_leader_address": s.getLeaderAddress(),
        "num_controllers":           s.raftNode.GetPeerCount(),
        "num_brokers":               info.NumBrokers,
        "num_topics":                info.NumTopics,
        "total_partitions":          info.TotalPartitions,
        "metadata_version":          info.MetadataVersion,
        "raft_term":                 s.raftNode.GetCurrentTerm(),
        "raft_commit_index":         s.raftNode.GetCommitIndex(),
    })
}

// Get Full Metadata (for workers)
func (s *HTTPServer) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
    // Get version from query param
    lastKnownVersion := int64(0)
    if v := r.URL.Query().Get("version"); v != "" {
        fmt.Sscanf(v, "%d", &lastKnownVersion)
    }
    
    metadata, err := s.metadataService.GetMetadata(r.Context(), lastKnownVersion)
    if err != nil {
        s.respondError(w, http.StatusInternalServerError, "metadata_failed", err.Error())
        return
    }
    
    s.respondJSON(w, http.StatusOK, metadata)
}

// Get Raft Status
func (s *HTTPServer) handleGetRaftStatus(w http.ResponseWriter, r *http.Request) {
    status := s.raftNode.GetStatus()
    
    s.respondJSON(w, http.StatusOK, status)
}

// Health Check
func (s *HTTPServer) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
    healthy := s.raftNode.IsHealthy()
    
    if healthy {
        s.respondJSON(w, http.StatusOK, map[string]interface{}{
            "status": "healthy",
            "state":  s.raftNode.GetState(),
        })
    } else {
        s.respondJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
            "status": "unhealthy",
            "state":  s.raftNode.GetState(),
        })
    }
}
```

### Helper Functions

```go
func (s *HTTPServer) respondJSON(w http.ResponseWriter, status int, data interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    json.NewEncoder(w).Encode(data)
}

func (s *HTTPServer) respondError(w http.ResponseWriter, status int, errorCode, message string) {
    s.respondJSON(w, status, map[string]interface{}{
        "success": false,
        "error":   errorCode,
        "message": message,
    })
}

func (s *HTTPServer) getLeaderAddress() string {
    leader := s.raftNode.GetLeader()
    if leader == "" {
        return ""
    }
    
    // Get leader's HTTP address from config
    // This would come from peer configuration
    return fmt.Sprintf("http://%s:8080", leader)
}
```

---

## Integration with Existing Services

### Service Layer (Shared by HTTP and gRPC)

```go
type MetadataService struct {
    stateMachine *MetadataStateMachine
    raft         *RaftNode
}

func NewMetadataService(sm *MetadataStateMachine, raft *RaftNode) *MetadataService {
    return &MetadataService{
        stateMachine: sm,
        raft:         raft,
    }
}

// CreateTopic - shared by both HTTP and gRPC
func (s *MetadataService) CreateTopic(ctx context.Context, req *CreateTopicRequest) (*Topic, error) {
    // Validate
    if req.NumPartitions <= 0 {
        return nil, fmt.Errorf("num_partitions must be positive")
    }
    
    // Check if leader
    if !s.raft.IsLeader() {
        return nil, ErrNotLeader
    }
    
    // Create command
    cmd := CreateTopicCommand{
        TopicID:           generateUUID(),
        Name:              req.Name,
        NumPartitions:     req.NumPartitions,
        ReplicationFactor: req.ReplicationFactor,
        Config:            parseTopicConfig(req.Config),
    }
    
    data, _ := json.Marshal(cmd)
    
    // Create log entry
    entry := &LogEntry{
        Type:      EntryTypeCreateTopic,
        Timestamp: time.Now().Format(time.RFC3339),
        Data:      data,
    }
    
    // Propose to Raft
    if err := s.raft.Propose(entry); err != nil {
        return nil, err
    }
    
    // Wait for commit (with timeout from context)
    if err := s.waitForApply(ctx, entry); err != nil {
        return nil, err
    }
    
    // Retrieve created topic from state machine
    topic := s.stateMachine.GetTopic(req.Name)
    
    return topic, nil
}

// Similar methods for other operations...
```

### Main Application (Starting Both Servers)

```go
package main

import (
    "log"
    "sync"
)

func main() {
    // Load config
    config := loadConfig("controller-1.json")
    
    // Initialize storage
    storage, err := NewJSONStorage(config.DataDir)
    if err != nil {
        log.Fatal(err)
    }
    
    // Initialize state machine
    stateMachine := NewMetadataStateMachine()
    
    // Initialize Raft node
    raftNode := NewRaftNode(config.NodeID, config.RaftPort, storage, stateMachine)
    
    // Initialize metadata service (shared by HTTP and gRPC)
    metadataService := NewMetadataService(stateMachine, raftNode)
    
    // Start Raft node
    go raftNode.Start()
    
    // Start gRPC server
    grpcServer := NewGRPCServer(metadataService, raftNode, config.GRPCPort)
    go func() {
        if err := grpcServer.Start(); err != nil {
            log.Fatal("gRPC server failed:", err)
        }
    }()
    
    // Start HTTP server
    httpServer := NewHTTPServer(metadataService, raftNode, config.HTTPPort)
    go func() {
        if err := httpServer.Start(); err != nil {
            log.Fatal("HTTP server failed:", err)
        }
    }()
    
    log.Printf("Controller %s started", config.NodeID)
    log.Printf("- HTTP API: http://localhost:%d", config.HTTPPort)
    log.Printf("- gRPC API: localhost:%d", config.GRPCPort)
    log.Printf("- Raft: localhost:%d", config.RaftPort)
    
    // Wait forever
    select {}
}
```

---

## Testing with curl

### Create a Topic

```bash
curl -X POST http://localhost:8080/api/v1/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders",
    "num_partitions": 10,
    "replication_factor": 3,
    "config": {
      "retention.ms": "604800000",
      "cleanup.policy": "delete"
    }
  }'
```

### List Topics

```bash
curl http://localhost:8080/api/v1/topics
```

### Get Topic Details

```bash
curl http://localhost:8080/api/v1/topics/orders
```

### Update Topic Config

```bash
curl -X PUT http://localhost:8080/api/v1/topics/orders \
  -H "Content-Type: application/json" \
  -d '{
    "config": {
      "retention.ms": "1209600000"
    }
  }'
```

### Delete Topic

```bash
curl -X DELETE http://localhost:8080/api/v1/topics/orders
```

### Register Broker

```bash
curl -X POST http://localhost:8080/api/v1/brokers \
  -H "Content-Type: application/json" \
  -d '{
    "broker_id": "broker-1",
    "host": "192.168.1.101",
    "port": 9092,
    "rack": "rack-1",
    "endpoints": {
      "plaintext": "192.168.1.101:9092",
      "grpc": "192.168.1.101:9093"
    }
  }'
```

### Send Heartbeat

```bash
curl -X POST http://localhost:8080/api/v1/brokers/broker-1/heartbeat \
  -H "Content-Type: application/json" \
  -d '{
    "resources": {
      "disk_total_bytes": 1099511627776,
      "disk_used_bytes": 549755813888,
      "cpu_usage_percent": 45.5,
      "network_throughput_mbps": 125.3
    }
  }'
```

### Get Cluster Info

```bash
curl http://localhost:8080/api/v1/cluster/info
```

### Get Raft Status

```bash
curl http://localhost:8080/api/v1/raft/status
```

### Health Check

```bash
curl http://localhost:8080/health
```

---

## Alternative: Using grpc-gateway

If you want to auto-generate HTTP/JSON API from gRPC proto files:

### 1. Update Proto File

```protobuf
syntax = "proto3";

package metadata;

import "google/api/annotations.proto";

service MetadataService {
  rpc CreateTopic(CreateTopicRequest) returns (CreateTopicResponse) {
    option (google.api.http) = {
      post: "/api/v1/topics"
      body: "*"
    };
  }
  
  rpc ListTopics(ListTopicsRequest) returns (ListTopicsResponse) {
    option (google.api.http) = {
      get: "/api/v1/topics"
    };
  }
  
  rpc GetTopic(GetTopicRequest) returns (GetTopicResponse) {
    option (google.api.http) = {
      get: "/api/v1/topics/{name}"
    };
  }
  
  // ... other endpoints
}
```

### 2. Generate Gateway Code

```bash
# Install tools
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest

# Generate
protoc -I . \
  --grpc-gateway_out . \
  --grpc-gateway_opt logtostderr=true \
  metadata.proto
```

### 3. Start Gateway

```go
func runGateway(grpcPort, httpPort int) error {
    ctx := context.Background()
    
    mux := runtime.NewServeMux()
    opts := []grpc.DialOption{grpc.WithInsecure()}
    
    endpoint := fmt.Sprintf("localhost:%d", grpcPort)
    
    err := gw.RegisterMetadataServiceHandlerFromEndpoint(ctx, mux, endpoint, opts)
    if err != nil {
        return err
    }
    
    return http.ListenAndServe(fmt.Sprintf(":%d", httpPort), mux)
}
```

**Benefits:**
- Auto-generated from proto
- Keeps HTTP and gRPC in sync
- Less code to maintain

**Drawbacks:**
- Less control over HTTP responses
- Additional dependency
- Learning curve

---

## Error Handling

### Standard Error Response

```json
{
  "success": false,
  "error": "error_code",
  "message": "Human readable message",
  "field": "field_name",
  "details": {}
}
```

### Common Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `validation_error` | 400 | Invalid request parameters |
| `topic_not_found` | 404 | Topic does not exist |
| `broker_not_found` | 404 | Broker does not exist |
| `topic_exists` | 409 | Topic already exists |
| `not_leader` | 307 | Node is not leader (redirect) |
| `no_leader` | 503 | No leader elected |
| `raft_timeout` | 504 | Raft consensus timeout |
| `internal_error` | 500 | Internal server error |

---

## Configuration

### Controller Config with HTTP Port

```json
{
  "node_id": "controller-1",
  "role": "controller",
  "http_port": 8080,
  "grpc_port": 8082,
  "raft_port": 8081,
  "data_dir": "./data/controller-1",
  "peers": [
    {
      "id": "controller-2",
      "http_address": "http://controller-2:8080",
      "grpc_address": "controller-2:8082",
      "raft_address": "controller-2:8081"
    }
  ]
}
```

---

## Summary

**HTTP API Layer:**
- REST interface for all metadata operations
- Wraps same service layer as gRPC
- Easy testing with curl/Postman
- Leader redirection for write operations

**Key Features:**
- Middleware: logging, CORS, leader check
- Proper error handling with codes
- JSON request/response
- Health checks

**Integration:**
- Shared service layer (HTTP + gRPC → MetadataService → Raft)
- Both servers run simultaneously
- Same business logic, different transport

**Testing:**
- curl commands for all operations
- Easy local development
- Browser-accessible

This gives you the best of both worlds: gRPC for performance (broker-controller communication) and HTTP/REST for ease of use (admin operations, debugging, web UI)!
