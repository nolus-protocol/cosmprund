# Cosmos-Pruner

A tool for pruning Cosmos SDK node data with data-aware algorithms and comprehensive logging. Reduces disk usage by removing old blockchain data while preserving chain integrity.

## Key Features

### Data-Aware Pruning
- **Range Detection**: Automatically discovers available data ranges before pruning
- **Gap-Tolerant Processing**: Handles missing or sparse data without errors
- **Sample-Based Validation**: Uses data sampling to validate ranges and avoid unnecessary operations
- **Chunk-Based Processing**: Processes data in configurable batches to prevent memory issues

### Comprehensive Data Store Support
- **Application State**: Prunes Cosmos SDK app state with multi-store support
- **Block Store**: Removes old blocks while maintaining chain continuity
- **State Store**: Cleans up validator and consensus state data
- **Transaction Index**: Prunes tx_index and block_events for space savings

### Logging & Monitoring
- **Multi-Level Logging**: Error, Warning, Info, and Debug levels
- **Progress Reporting**: Real-time progress updates for long-running operations
- **Batch Logging**: Optional detailed batch processing logs
- **Quiet Mode**: Minimal output mode for automated environments

### Database Optimization
- **LevelDB Settings**: Uses optimized LevelDB settings for performance
- **Optional Compaction**: Post-pruning database compaction to reclaim space
- **Batch Operations**: Efficient batch writes to minimize I/O overhead

## Installation

```bash
# Clone and build
git clone <repository-url>
cd cosmos-pruner
go build -o cosmos-pruner main.go
```

## Usage

### Basic Usage
```bash
./cosmos-pruner prune ~/.gaiad
```

### Advanced Usage with Custom Settings
```bash
./cosmos-pruner prune ~/.gaiad \
  --blocks=100000 \
  --versions=50000 \
  --verbose \
  --compact=true
```

### Production Environment (Quiet Mode)
```bash
./cosmos-pruner prune ~/.gaiad \
  --blocks=362880 \
  --versions=362880 \
  --quiet \
  --compact=true
```

## Configuration Flags

### Core Pruning Options
| Flag | Default | Description |
|------|---------|-------------|
| `--blocks` | `10` | Number of recent blocks to retain |
| `--versions` | `10` | Number of app state versions to keep |
| `--data-dir` | `data` | Path to node data directory |

### Component Selection
| Flag | Default | Description | Database(s) Affected |
|------|---------|-------------|---------------------|
| `--cosmos-sdk` | `true` | Enable app state pruning | `application.db` |
| `--tendermint` | `true` | Enable block store and state pruning | `blockstore.db`, `state.db` |
| `--tx_index` | `true` | Enable transaction index pruning | `tx_index.db` |
| `--compact` | `true` | Compact databases after pruning | All enabled databases |

### Logging & Output
| Flag | Default | Description |
|------|---------|-------------|
| `--verbose` | `false` | Enable detailed debug output |
| `--batch-logs` | `false` | Show batch processing details |
| `--quiet` | `false` | Suppress all non-error messages |

### Legacy Options
| Flag | Default | Description |
|------|---------|-------------|
| `--app` | `""` | App name (legacy, rarely needed) |

## Database Components

The tool processes four main database chunks in your Cosmos SDK node:

### 1. Application State (`application.db`)
**What it stores**: App state versions, store data, IAVL tree nodes
**Pruning logic**: Removes versions older than `--versions` parameter
**Skipped when**:
- No latest version found in database
- No store keys discovered (empty or corrupted database)
- Insufficient versions (latest version ≤ retention setting)
- Cannot load any working version of the store

### 2. Block Store (`blockstore.db`)
**What it stores**: Raw block data, block metadata, block parts
**Pruning logic**: Removes blocks older than `--blocks` parameter
**Skipped when**:
- Current height ≤ retention blocks setting
- Block store already pruned sufficiently (base height covers retention)

### 3. State Store (`state.db`)
**What it stores**: Validator sets, consensus states, blockchain state metadata
**Pruning logic**: Uses block store height as guide, prunes corresponding state data
**Skipped when**:
- No state data found at sample heights (empty database)
- Too many errors with minimal progress (indicates sparse/missing data)

### 4. Transaction Index (`tx_index.db`)
**What it stores**: Transaction-to-block mappings, event indices, block event data
**Pruning logic**: Removes indices for blocks older than retention + 10 block buffer
**Skipped when**:
- Calculated prune height ≤ 0 (insufficient data to determine safe pruning point)

## How It Works

### 1. Data Discovery Phase
- Scans each database to identify available data ranges
- Validates data integrity at sample points
- Calculates optimal pruning boundaries per component
- Reports which databases will be processed or skipped

### 2. Pruning Phase
- Processes each database component in sequence
- Uses configurable batch sizes (1000 items default for most operations)
- Handles errors without stopping (important for sparse state data)
- Provides real-time progress updates per database

### 3. Optimization Phase
- Optional database compaction to reclaim disk space
- Validates pruning results across all components
- Reports final space savings per database

## Example Scenarios

### Mainnet Node (Conservative)
Keep 30 days of blocks (~362,880 blocks at 7.5s block time):
```bash
./cosmos-pruner prune ~/.gaiad --blocks=362880 --versions=362880
```

### Archive Node Cleanup
Remove very old data while keeping substantial history:
```bash
./cosmos-pruner prune ~/.gaiad --blocks=1000000 --versions=500000 --verbose
```

### Storage Emergency
Minimal retention for immediate space relief:
```bash
./cosmos-pruner prune ~/.gaiad --blocks=1000 --versions=1000 --compact=true
```

### Automated Maintenance
Silent operation for cron jobs:
```bash
./cosmos-pruner prune ~/.gaiad --blocks=100000 --versions=100000 --quiet
```

## Safety Features

- **Non-Destructive Validation**: Checks data integrity before pruning
- **Graceful Error Handling**: Continues operation despite minor errors
- **Batch Size Limiting**: Prevents memory overflow on large datasets
- **Progress Checkpointing**: Shows progress to monitor long operations

## Performance Characteristics

- **Memory Efficient**: Constant memory usage regardless of dataset size
- **I/O Optimized**: Batched operations minimize disk thrashing
- **Interrupt Safe**: Can be safely stopped and restarted
- **Progress Transparent**: Clear visibility into operation status

## Understanding the Output

### Normal Operation Messages
```
pruning application state
Found latest app version: 1250000
App state: will attempt to prune versions 1 to 1240000, keeping 1240001 to 1250000
Pruning 1240000 versions from app state

pruning block store
Block store: will prune blocks up to height 1240000, keeping 1240001 to 1250000

pruning state store
State store: will attempt to prune states up to height 1240000 based on block store info

pruning tx_index and block index
finished pruning block index
finished pruning tx_index
```

### Skip Messages and Reasons
```
# App state skipped - not enough versions
App state has latest version 5000, retention is 10000 - no pruning needed

# Block store skipped - already pruned
Not enough blocks for pruning (current: 8000, retention: 10000)
Block store already pruned sufficiently

# State store skipped - no data
No state data found at sample heights, skipping state store pruning

# TX index skipped - insufficient height
No need to prune tx_index (pruneHeight=-5000)
```

### Error Messages (Normal for Sparse Data)
```
# State store errors (expected)
State chunk 100000-101000 had errors (expected for sparse data)
Too many state pruning errors with little progress - likely sparse data, stopping

# Version errors in app state (may be normal)
cannot delete latest saved version (continuing with next version)
```

## Monitoring & Troubleshooting

### Enable Verbose Logging
```bash
./cosmos-pruner prune ~/.gaiad --verbose
```

### Monitor Batch Processing
```bash
./cosmos-pruner prune ~/.gaiad --batch-logs
```

### Check Database State
The tool validates data ranges before pruning and reports:
- Available block ranges
- App state version ranges
- Data gaps or inconsistencies
- Estimated space to be freed

## Requirements

- **Go**: 1.19 or later
- **Disk Space**: At least 20% free space during compaction
- **Permissions**: Read/write access to node data directory
- **Downtime**: Node should be stopped during pruning

## Contributing

This tool is designed for production use with Cosmos SDK chains. Test thoroughly on non-production data before using on mainnet nodes.
