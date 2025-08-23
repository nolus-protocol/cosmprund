// File: cmd/pruner.go
package cmd

import (
	"encoding/binary"
	"fmt"
	storetypes "github.com/cosmos/cosmos-sdk/store/types"
	"path/filepath"
	"strconv"
	"strings"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/spf13/cobra"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tendermint/tendermint/state"
	tmstore "github.com/tendermint/tendermint/store"
	db "github.com/tendermint/tm-db"

	"github.com/binaryholdings/cosmos-pruner/internal/rootmulti"
)

// to figuring out the height to prune tx_index
var txIdxHeight int64 = 0

// DataRange represents a range of available data
type DataRange struct {
	Min int64
	Max int64
}

func (dr *DataRange) IsValid() bool {
	return dr.Min > 0 && dr.Max > 0 && dr.Max >= dr.Min
}

func (dr *DataRange) Size() int64 {
	if !dr.IsValid() {
		return 0
	}
	return dr.Max - dr.Min + 1
}

func pruneCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "prune [path_to_home]",
		Short: "prune data from the application store and block store",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {

			//ctx := cmd.Context()
			//errs, _ := errgroup.WithContext(ctx)
			var err error
			if tendermint {
				if err = pruneTMData(args[0]); err != nil {
					logger.Error("Failed to prune tendermint data: %v", err)
				}
			}

			if cosmosSdk {
				err = pruneAppState(args[0])
				if err != nil {
					logger.Error("Failed to prune cosmos-sdk data: %v", err)
				}
			}

			if tx_idx {
				err = pruneTxIndex(args[0])
				if err != nil {
					logger.Error("Failed to prune tx_index: %v", err)
				}
			}

			return nil
		},
	}
	return cmd
}

func pruneTxIndex(home string) error {
	logger.Info("pruning tx_index and block index")
	txIdxDB, err := openDB("tx_index", home)
	if err != nil {
		return err
	}

	defer func() {
		errClose := txIdxDB.Close()
		if errClose != nil {
			logger.Error("Failed to close tx_index database: %v", errClose)
		}
	}()

	pruneHeight := txIdxHeight - int64(blocks) - 10
	if pruneHeight <= 0 {
		logger.Info("No need to prune tx_index (pruneHeight=%d)", pruneHeight)
		return nil
	}

	pruneBlockIndex(txIdxDB, pruneHeight)
	logger.Info("finished pruning block index")

	pruneTxIndexTxs(txIdxDB, pruneHeight)
	logger.Info("finished pruning tx_index")

	if compact {
		logger.Info("compacting tx_index")
		if err := compactDB(txIdxDB); err != nil {
			logger.Error("Failed to compact tx_index: %v", err)
		}
	}

	return nil
}

func pruneTxIndexTxs(db db.DB, pruneHeight int64) {
	itr, itrErr := db.Iterator(nil, nil)
	if itrErr != nil {
		panic(itrErr)
	}

	defer itr.Close()

	///////////////////////////////////////////////////
	// delete index by hash and index by height

	bat := db.NewBatch()
	counter := 0
	totalProcessed := 0

	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()

		strKey := string(key)

		if strings.HasPrefix(strKey, "tx.height") { // index by height
			strs := strings.Split(strKey, "/")
			intHeight, _ := strconv.ParseInt(strs[2], 10, 64)

			if intHeight < pruneHeight {
				//db.Delete(value)
				//db.Delete(key)
				bat.Delete(value)
				bat.Delete(key)
				counter += 2
			}
		} else {
			if len(value) == 32 { // maybe index tx by events
				strs := strings.Split(strKey, "/")
				if len(strs) == 4 { // index tx by events
					intHeight, _ := strconv.ParseInt(strs[2], 10, 64)
					if intHeight < pruneHeight {
						//db.Delete(key)
						//db.DeleteSync(key)
						bat.Delete(key)
						counter++
					}
				}
			}
		}

		if counter >= 1000 {
			totalProcessed += counter
			logger.BatchProgress("tx_index", counter, totalProcessed)
			bat.WriteSync()
			counter = 0
			bat.Close()
			bat = db.NewBatch()
		}
	}

	if counter > 0 {
		totalProcessed += counter
		logger.BatchProgress("tx_index", counter, totalProcessed)
	}

	bat.WriteSync()
	bat.Close()
}

func pruneBlockIndex(db db.DB, pruneHeight int64) {
	itr, itrErr := db.Iterator(nil, nil)
	if itrErr != nil {
		panic(itrErr)
	}

	defer itr.Close()

	bat := db.NewBatch()
	counter := 0
	totalProcessed := 0

	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()

		strKey := string(key)

		if strings.HasPrefix(strKey, "block.height") /* index block primary key*/ || strings.HasPrefix(strKey, "block_events") /* BeginBlock & EndBlock */ {
			intHeight := int64FromBytes(value)
			//fmt.Printf("intHeight: %d\n", intHeight)

			if intHeight < pruneHeight {
				//db.Delete(key)
				//db.DeleteSync(key)
				bat.Delete(key)
				counter++
			}
		}

		if counter >= 1000 {
			totalProcessed += counter
			logger.BatchProgress("block_index", counter, totalProcessed)
			bat.WriteSync()
			counter = 0
			bat.Close()
			bat = db.NewBatch()
		}
	}

	if counter > 0 {
		totalProcessed += counter
		logger.BatchProgress("block_index", counter, totalProcessed)
	}

	bat.WriteSync()
	bat.Close()
}

func pruneAppState(home string) error {
	logger.Info("pruning application state")

	appDB, errDB := openDB("application", home)
	if errDB != nil {
		return errDB
	}

	defer appDB.Close()

	// Check if database has any data
	keys, err := getStoreKeysWithValidation(appDB)
	if err != nil {
		logger.Warn("Could not get store keys from database: %v", err)
		logger.Info("Database might be empty or corrupted, skipping app state pruning")
		return nil
	}

	if len(keys) == 0 {
		logger.Info("No store keys found, skipping app state pruning")
		return nil
	}

	logger.Debug("Found store keys: %v", keys)

	// DISCOVER what app state versions actually exist - get valid versions list
	validVersions, err := discoverValidAppVersions(appDB)
	if err != nil {
		logger.Warn("Could not discover valid app versions: %v", err)
		logger.Info("Skipping application state pruning - unable to determine available versions")
		return nil
	}

	if len(validVersions) == 0 {
		logger.Info("No valid versions found, skipping app state pruning")
		return nil
	}

	minVersion := validVersions[0]
	maxVersion := validVersions[len(validVersions)-1]

	logger.Info("App state: found %d valid versions from %d to %d",
		len(validVersions), minVersion, maxVersion)

	// Set txIdxHeight from discovered app versions
	if txIdxHeight <= 0 {
		txIdxHeight = maxVersion
		logger.Debug("set txIdxHeight=%d from discovered app state versions", txIdxHeight)
	}

	// Check if we have enough versions to prune
	if len(validVersions) <= versions {
		logger.Info("App state has %d versions, retention is %d - no pruning needed",
			len(validVersions), versions)
		return nil
	}

	// Calculate what to prune: keep last N versions of existing data
	keepFromIndex := len(validVersions) - versions
	versionsToKeep := validVersions[keepFromIndex:]
	versionsToPrune := validVersions[:keepFromIndex]

	if len(versionsToPrune) == 0 {
		logger.Info("No versions to prune after calculations")
		return nil
	}

	logger.Info("App state: will prune %d versions (from %d to %d), keeping %d versions (from %d to %d)",
		len(versionsToPrune), versionsToPrune[0], versionsToPrune[len(versionsToPrune)-1],
		len(versionsToKeep), versionsToKeep[0], versionsToKeep[len(versionsToKeep)-1])

	// Mount stores and prune using discovered versions
	appStore := rootmulti.NewStore(appDB)
	for _, key := range keys {
		appStore.MountStoreWithDB(storetypes.NewKVStoreKey(key), sdk.StoreTypeIAVL, nil)
	}

	// Load latest valid version that actually exists
	err = appStore.LoadVersion(maxVersion)
	if err != nil {
		logger.Warn("Failed to load version %d: %v", maxVersion, err)
		logger.Info("Falling back to LoadLatestVersion")
		err = appStore.LoadLatestVersion()
		if err != nil {
			return fmt.Errorf("failed to load any version: %w", err)
		}
	}

	logger.Info("Pruning %d valid versions from app state", len(versionsToPrune))
	appStore.PruneHeights = versionsToPrune
	appStore.PruneStores()

	if compact {
		logger.Info("compacting application state")
		if err := compactDB(appDB); err != nil {
			logger.Error("Failed to compact application state: %v", err)
		}
	}

	return nil
}

// pruneTMData prunes the tendermint blocks and state based on available data
func pruneTMData(home string) error {
	blockStoreDB, errDBBlock := openDB("blockstore", home)
	if errDBBlock != nil {
		return errDBBlock
	}

	blockStore := tmstore.NewBlockStore(blockStoreDB)
	defer blockStore.Close()

	// Get StateStore
	stateDB, errDBBState := openDB("state", home)
	if errDBBState != nil {
		return errDBBState
	}

	stateStore := state.NewStore(stateDB)
	defer stateStore.Close()

	currentHeight := blockStore.Height()
	base := blockStore.Base()

	logger.Debug("Block store: base=%d, current=%d", base, currentHeight)

	if currentHeight <= int64(blocks) {
		logger.Info("Not enough blocks for pruning (current: %d, retention: %d)", currentHeight, blocks)
		return nil
	}

	if txIdxHeight <= 0 {
		txIdxHeight = currentHeight
		logger.Debug("set txIdxHeight=%d from block store", txIdxHeight)
	}

	// BLOCK STORE PRUNING (already data-aware)
	logger.Info("pruning block store")

	blockPruneHeight := currentHeight - int64(blocks)
	logger.Info("Block store: will prune blocks up to height %d, keeping %d to %d",
		blockPruneHeight, blockPruneHeight+1, currentHeight)

	if blockPruneHeight > base {
		for pruneFrom := base; pruneFrom < blockPruneHeight; pruneFrom += rootmulti.PRUNE_BATCH_SIZE {
			height := pruneFrom + rootmulti.PRUNE_BATCH_SIZE - 1
			if height > blockPruneHeight {
				height = blockPruneHeight
			}

			_, err := blockStore.PruneBlocks(height)
			if err != nil {
				logger.Error("Failed to prune blocks at height %d: %v", height, err)
			}
		}
	} else {
		logger.Info("Block store already pruned sufficiently")
	}

	if compact {
		logger.Info("compacting block store")
		if err := compactDB(blockStoreDB); err != nil {
			logger.Error("Failed to compact block store: %v", err)
		}
	}

	// ROBUST STATE STORE PRUNING
	logger.Info("pruning state store")

	// Discover what state data actually exists - get valid heights list
	validHeights, err := discoverValidStateHeights(stateDB)
	if err != nil {
		logger.Warn("Could not discover valid state heights: %v", err)
		logger.Info("Skipping state store pruning - unable to determine available heights")
		return nil
	}

	if len(validHeights) == 0 {
		logger.Info("No valid state heights found, skipping state store pruning")
		return nil
	}

	minHeight := validHeights[0]
	maxHeight := validHeights[len(validHeights)-1]

	logger.Info("State store: found %d valid heights from %d to %d",
		len(validHeights), minHeight, maxHeight)

	// Check if we have enough state data to prune
	if len(validHeights) <= blocks {
		logger.Info("State store has %d heights, retention is %d - no pruning needed",
			len(validHeights), blocks)
		return nil
	}

	// Calculate what to prune: keep last N blocks of existing state data
	keepFromIndex := len(validHeights) - blocks
	heightsToKeep := validHeights[keepFromIndex:]
	heightsToPrune := validHeights[:keepFromIndex]

	if len(heightsToPrune) == 0 {
		logger.Info("No heights to prune after calculations")
		return nil
	}

	logger.Info("State store: will prune %d heights (from %d to %d), keeping %d heights (from %d to %d)",
		len(heightsToPrune), heightsToPrune[0], heightsToPrune[len(heightsToPrune)-1],
		len(heightsToKeep), heightsToKeep[0], heightsToKeep[len(heightsToKeep)-1])

	// Prune in small batches within the valid heights only
	batchSize := int64(100)
	totalPruned := 0

	for i := 0; i < len(heightsToPrune); i += int(batchSize) {
		endIdx := i + int(batchSize)
		if endIdx > len(heightsToPrune) {
			endIdx = len(heightsToPrune)
		}

		batchHeights := heightsToPrune[i:endIdx]
		if len(batchHeights) == 0 {
			continue
		}

		minBatch := batchHeights[0]
		maxBatch := batchHeights[len(batchHeights)-1]

		logger.Debug("Pruning state batch from %d to %d (%d heights)", minBatch, maxBatch, len(batchHeights))

		err = stateStore.PruneStates(minBatch, maxBatch)
		if err != nil {
			logger.Error("Failed to prune state batch from %d to %d: %v", minBatch, maxBatch, err)
			// Continue with next batch rather than stopping entirely
			continue
		}

		totalPruned += len(batchHeights)

		// Progress logging
		if totalPruned%1000 == 0 || i+int(batchSize) >= len(heightsToPrune) {
			logger.Progress("State pruning progress: %d/%d heights processed", totalPruned, len(heightsToPrune))
		}
	}

	logger.Info("State pruning completed: %d heights processed", totalPruned)

	if compact {
		logger.Info("compacting state store")
		if err := compactDB(stateDB); err != nil {
			logger.Error("Failed to compact state store: %v", err)
		}
	}

	return nil
}

// discoverValidAppVersions finds all actually accessible app state versions
func discoverValidAppVersions(appDB db.DB) ([]int64, error) {
	itr, err := appDB.Iterator(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer itr.Close()

	versionSet := make(map[int64]bool)
	commitInfoPrefix := "s/"

	for ; itr.Valid(); itr.Next() {
		key := string(itr.Key())

		// Look for commit info keys (format: s/<version>)
		if strings.HasPrefix(key, commitInfoPrefix) && key != "s/latest" && key != "s/pruneheights" {
			versionStr := strings.TrimPrefix(key, commitInfoPrefix)
			if version, parseErr := strconv.ParseInt(versionStr, 10, 64); parseErr == nil && version > 0 {
				// Validate this version is actually accessible by trying to load commit info
				if _, checkErr := getCommitInfo(appDB, version); checkErr == nil {
					versionSet[version] = true
				}
			}
		}
	}

	if len(versionSet) == 0 {
		return nil, fmt.Errorf("no accessible commit info versions found")
	}

	// Convert to sorted slice
	var versions []int64
	for version := range versionSet {
		versions = append(versions, version)
	}

	// Sort versions
	for i := 0; i < len(versions)-1; i++ {
		for j := i + 1; j < len(versions); j++ {
			if versions[i] > versions[j] {
				versions[i], versions[j] = versions[j], versions[i]
			}
		}
	}

	return versions, nil
}

// discoverValidStateHeights finds all actually accessible state heights
func discoverValidStateHeights(stateDB db.DB) ([]int64, error) {
	itr, err := stateDB.Iterator(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer itr.Close()

	heightSet := make(map[int64]bool)

	// Look for state keys that contain height information
	for ; itr.Valid(); itr.Next() {
		key := string(itr.Key())

		// Look for various state keys that contain height information
		var height int64
		found := false

		// Check for validator keys (format: validatorsKey:<height>)
		if strings.Contains(key, "validatorsKey:") {
			parts := strings.Split(key, ":")
			if len(parts) >= 2 {
				if h, parseErr := strconv.ParseInt(parts[len(parts)-1], 10, 64); parseErr == nil && h > 0 {
					height = h
					found = true
				}
			}
		}

		// Check for consensus state keys (format: consensusState:<height>)
		if !found && strings.Contains(key, "consensusState:") {
			parts := strings.Split(key, ":")
			if len(parts) >= 2 {
				if h, parseErr := strconv.ParseInt(parts[len(parts)-1], 10, 64); parseErr == nil && h > 0 {
					height = h
					found = true
				}
			}
		}

		// Check for other height-based keys
		if !found && strings.Contains(key, "Height:") {
			parts := strings.Split(key, ":")
			for _, part := range parts {
				if h, parseErr := strconv.ParseInt(part, 10, 64); parseErr == nil && h > 0 {
					height = h
					found = true
					break
				}
			}
		}

		if found {
			heightSet[height] = true
		}
	}

	if len(heightSet) == 0 {
		return nil, fmt.Errorf("no state data with height information found")
	}

	// Convert to sorted slice
	var heights []int64
	for height := range heightSet {
		heights = append(heights, height)
	}

	// Sort heights
	for i := 0; i < len(heights)-1; i++ {
		for j := i + 1; j < len(heights); j++ {
			if heights[i] > heights[j] {
				heights[i], heights[j] = heights[j], heights[i]
			}
		}
	}

	return heights, nil
}

// Utils

func openDB(dbname string, home string) (db.DB, error) {
	dbDir := rootify(dataDir, home)

	// LevelDB with optimized options
	o := opt.Options{
		DisableSeeksCompaction: true,
	}

	lvlDB, err := db.NewGoLevelDBWithOpts(dbname, dbDir, &o)
	if err != nil {
		return nil, err
	}

	return lvlDB, nil
}

func compactDB(vdb db.DB) error {
	// LevelDB compaction
	vdbLevel := vdb.(*db.GoLevelDB)
	return vdbLevel.ForceCompact(nil, nil)
}

// getStoreKeysWithValidation gets store keys with proper error handling
func getStoreKeysWithValidation(db db.DB) ([]string, error) {
	validVersions, err := discoverValidAppVersions(db)
	if err != nil {
		return nil, fmt.Errorf("failed to discover valid versions: %w", err)
	}

	if len(validVersions) == 0 {
		return nil, fmt.Errorf("no valid versions found")
	}

	// Use the latest valid version to get store keys
	latestVer := validVersions[len(validVersions)-1]

	latestCommitInfo, err := getCommitInfo(db, latestVer)
	if err != nil {
		return nil, fmt.Errorf("failed to get commit info for version %d: %w", latestVer, err)
	}

	if latestCommitInfo == nil {
		return nil, fmt.Errorf("commit info is nil for version %d", latestVer)
	}

	if len(latestCommitInfo.StoreInfos) == 0 {
		return nil, fmt.Errorf("no store infos found in commit info for version %d", latestVer)
	}

	var storeKeys []string
	for _, storeInfo := range latestCommitInfo.StoreInfos {
		storeKeys = append(storeKeys, storeInfo.Name)
	}

	return storeKeys, nil
}

// getStoreKeys (legacy function for backward compatibility)
func getStoreKeys(db db.DB) (storeKeys []string) {
	keys, err := getStoreKeysWithValidation(db)
	if err != nil {
		panic(err)
	}
	return keys
}

func getCommitInfo(db db.DB, ver int64) (*storetypes.CommitInfo, error) {
	const commitInfoKeyFmt = "s/%d" // s/<version>
	cInfoKey := fmt.Sprintf(commitInfoKeyFmt, ver)

	bz, err := db.Get([]byte(cInfoKey))
	if err != nil {
		return nil, fmt.Errorf("failed to get commit info: %s", err)
	} else if bz == nil {
		return nil, fmt.Errorf("no commit info found")
	}

	cInfo := &storetypes.CommitInfo{}
	if err = cInfo.Unmarshal(bz); err != nil {
		return nil, fmt.Errorf("failed unmarshal commit info: %s", err)
	}

	return cInfo, nil
}

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}

func rootify(path, root string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}

func int64FromBytes(bz []byte) int64 {
	v, _ := binary.Varint(bz)
	return v
}
