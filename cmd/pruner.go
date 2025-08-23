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

	// Quick method: try to get latest version and store keys directly
	latestVer := rootmulti.GetLatestVersion(appDB)
	if latestVer <= 0 {
		logger.Info("No latest version found, skipping app state pruning")
		return nil
	}

	logger.Debug("Found latest app version: %d", latestVer)

	// Check if database has any data by getting store keys
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

	// Set txIdxHeight from latest version
	if txIdxHeight <= 0 {
		txIdxHeight = latestVer
		logger.Debug("set txIdxHeight=%d from latest app version", txIdxHeight)
	}

	// Smart approach: use block-store guided range for app state
	// Most app versions should align with block heights
	if latestVer <= int64(versions) {
		logger.Info("App state has latest version %d, retention is %d - no pruning needed", latestVer, versions)
		return nil
	}

	// Calculate pruning range based on latest version
	versionsToKeepFrom := latestVer - int64(versions) + 1
	versionsToPruneUntil := versionsToKeepFrom - 1

	if versionsToPruneUntil <= 0 {
		logger.Info("No app versions to prune (pruneUntil=%d)", versionsToPruneUntil)
		return nil
	}

	logger.Info("App state: will attempt to prune versions 1 to %d, keeping %d to %d",
		versionsToPruneUntil, versionsToKeepFrom, latestVer)

	// Quick sampling to see if we have data in the range
	sampleVersions := []int64{1, versionsToPruneUntil / 2, versionsToPruneUntil, versionsToKeepFrom, latestVer}
	validSamples := 0

	for _, ver := range sampleVersions {
		if ver > 0 && hasCommitInfoAtVersion(appDB, ver) {
			validSamples++
		}
	}

	logger.Debug("Found commit info at %d/%d sample versions", validSamples, len(sampleVersions))

	// Build pruning list in chunks to avoid memory issues
	var prunedVersions []int64
	chunkSize := int64(10000) // Process 10k versions at a time

	for chunkStart := int64(1); chunkStart <= versionsToPruneUntil; chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize - 1
		if chunkEnd > versionsToPruneUntil {
			chunkEnd = versionsToPruneUntil
		}

		// Add this chunk to pruning list
		for v := chunkStart; v <= chunkEnd; v++ {
			prunedVersions = append(prunedVersions, v)
		}

		// Progress update for large ranges
		if chunkEnd%100000 == 0 || chunkEnd >= versionsToPruneUntil {
			logger.Debug("Prepared app versions for pruning: 1 to %d", chunkEnd)
		}
	}

	if len(prunedVersions) == 0 {
		logger.Info("No versions to prune")
		return nil
	}

	// Mount stores and prune using version range
	appStore := rootmulti.NewStore(appDB)
	for _, key := range keys {
		appStore.MountStoreWithDB(storetypes.NewKVStoreKey(key), sdk.StoreTypeIAVL, nil)
	}

	// Try to load a working version - start from latest and work backwards
	var workingVersion int64
	loadSuccess := false

	// Try the latest version first
	err = appStore.LoadVersion(latestVer)
	if err == nil {
		workingVersion = latestVer
		loadSuccess = true
		logger.Debug("Successfully loaded latest version %d", latestVer)
	} else {
		logger.Warn("Failed to load latest version %d: %v", latestVer, err)

		// Try a few versions before the latest
		for i := int64(1); i <= 10 && !loadSuccess; i++ {
			tryVersion := latestVer - i
			if tryVersion > 0 {
				logger.Debug("Trying to load version %d", tryVersion)
				loadErr := appStore.LoadVersion(tryVersion)
				if loadErr == nil {
					workingVersion = tryVersion
					loadSuccess = true
					logger.Info("Successfully loaded version %d (latest %d failed)", tryVersion, latestVer)
					break
				}
			}
		}
	}

	// Final fallback to LoadLatestVersion
	if !loadSuccess {
		logger.Info("All specific versions failed, trying LoadLatestVersion")
		err = appStore.LoadLatestVersion()
		if err != nil {
			return fmt.Errorf("failed to load any version including LoadLatestVersion: %w", err)
		}
		workingVersion = appStore.LastCommitID().Version
		logger.Info("LoadLatestVersion succeeded with version %d", workingVersion)
	}

	// Adjust pruning range based on working version if needed
	if workingVersion < latestVer {
		// Recalculate pruning range based on working version
		if workingVersion <= int64(versions) {
			logger.Info("Working version %d is too low for pruning (retention: %d)", workingVersion, versions)
			return nil
		}

		newVersionsToKeepFrom := workingVersion - int64(versions) + 1
		newVersionsToPruneUntil := newVersionsToKeepFrom - 1

		if newVersionsToPruneUntil <= 0 {
			logger.Info("No versions to prune after adjustment (pruneUntil=%d)", newVersionsToPruneUntil)
			return nil
		}

		// Rebuild pruning list with adjusted range
		prunedVersions = prunedVersions[:0] // Clear the slice
		for chunkStart := int64(1); chunkStart <= newVersionsToPruneUntil; chunkStart += chunkSize {
			chunkEnd := chunkStart + chunkSize - 1
			if chunkEnd > newVersionsToPruneUntil {
				chunkEnd = newVersionsToPruneUntil
			}

			for v := chunkStart; v <= chunkEnd; v++ {
				prunedVersions = append(prunedVersions, v)
			}
		}

		logger.Info("Adjusted pruning based on working version %d: will prune %d versions up to %d",
			workingVersion, len(prunedVersions), newVersionsToPruneUntil)
	}

	logger.Info("Pruning %d versions from app state (this may take time for large ranges)", len(prunedVersions))
	appStore.PruneHeights = prunedVersions
	appStore.PruneStores()

	if compact {
		logger.Info("compacting application state")
		if compactErr := compactDB(appDB); compactErr != nil {
			logger.Error("Failed to compact application state: %v", compactErr)
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

	// SMART STATE STORE PRUNING - Use block store info to guide state pruning
	logger.Info("pruning state store")

	// Use block store info to determine state pruning range
	// Most state data should align with block heights
	statePruneHeight := currentHeight - int64(blocks)

	logger.Info("State store: will attempt to prune states up to height %d based on block store info", statePruneHeight)

	// Quick validation: check if state data exists at key heights
	sampleHeights := []int64{base, base + 1000, statePruneHeight - 1000, statePruneHeight, currentHeight - 100, currentHeight}
	validSamples := 0

	for _, height := range sampleHeights {
		if height > 0 && hasStateDataAtHeight(stateDB, height) {
			validSamples++
		}
	}

	if validSamples == 0 {
		logger.Info("No state data found at sample heights, skipping state store pruning")
		return nil
	}

	logger.Debug("Found state data at %d/%d sample heights, proceeding with pruning", validSamples, len(sampleHeights))

	// Prune state in chunks, handling gaps gracefully
	chunkSize := int64(1000) // Process 1000 heights at a time
	totalPruned := 0
	totalErrors := 0

	for chunkStart := base; chunkStart < statePruneHeight; chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize - 1
		if chunkEnd > statePruneHeight {
			chunkEnd = statePruneHeight
		}

		logger.Debug("Processing state chunk: %d to %d", chunkStart, chunkEnd)

		// Try to prune this chunk
		chunkErr := stateStore.PruneStates(chunkStart, chunkEnd)
		if chunkErr != nil {
			totalErrors++
			logger.Debug("State chunk %d-%d had errors (expected for sparse data): %v", chunkStart, chunkEnd, chunkErr)

			// If too many errors, the range might be mostly empty
			if totalErrors > 10 && totalPruned < 100 {
				logger.Info("Too many state pruning errors with little progress - likely sparse data, stopping state pruning")
				break
			}
		} else {
			totalPruned += int(chunkEnd - chunkStart + 1)
		}

		// Progress update every 10k heights
		if (chunkStart-base)%10000 == 0 || chunkEnd >= statePruneHeight {
			processed := chunkEnd - base + 1
			total := statePruneHeight - base + 1
			logger.Progress("State pruning: processed %d/%d heights (errors: %d)", processed, total, totalErrors)
		}
	}

	logger.Info("State pruning completed: %d total heights processed, %d errors (errors are normal for sparse data)",
		totalPruned, totalErrors)

	if compact {
		logger.Info("compacting state store")
		if compactErr := compactDB(stateDB); compactErr != nil {
			logger.Error("Failed to compact state store: %v", compactErr)
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
	processed := 0

	logger.Debug("Scanning for valid app versions...")

	for ; itr.Valid(); itr.Next() {
		key := string(itr.Key())
		processed++

		// Progress logging every 10k keys
		if processed%10000 == 0 {
			logger.Debug("App version discovery: processed %d keys", processed)
		}

		// Look for commit info keys (format: s/<version>)
		if strings.HasPrefix(key, commitInfoPrefix) && key != "s/latest" && key != "s/pruneheights" {
			versionStr := strings.TrimPrefix(key, commitInfoPrefix)
			if version, parseErr := strconv.ParseInt(versionStr, 10, 64); parseErr == nil && version > 0 {
				// For performance, assume commit info keys are valid without re-checking
				// (we'll validate when actually loading)
				versionSet[version] = true
			}
		}
	}

	if len(versionSet) == 0 {
		return nil, fmt.Errorf("no accessible commit info versions found")
	}

	logger.Debug("Found %d potential app versions", len(versionSet))

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

// hasCommitInfoAtVersion checks if commit info exists at a specific version (quick check)
func hasCommitInfoAtVersion(appDB db.DB, version int64) bool {
	const commitInfoKeyFmt = "s/%d" // s/<version>
	cInfoKey := fmt.Sprintf(commitInfoKeyFmt, version)

	val, err := appDB.Get([]byte(cInfoKey))
	return err == nil && val != nil
}

// hasStateDataAtHeight checks if state data exists at a specific height (quick check)
func hasStateDataAtHeight(stateDB db.DB, height int64) bool {
	// Check for common state keys at this height
	heightStr := strconv.FormatInt(height, 10)

	// Try validator key format
	validatorKey := "validatorsKey:" + heightStr
	if val, err := stateDB.Get([]byte(validatorKey)); err == nil && val != nil {
		return true
	}

	// Try consensus state key format
	consensusKey := "consensusState:" + heightStr
	if val, err := stateDB.Get([]byte(consensusKey)); err == nil && val != nil {
		return true
	}

	return false
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
	// Try to get latest version first (fastest method)
	latestVer := rootmulti.GetLatestVersion(db)
	if latestVer > 0 {
		if latestCommitInfo, err := getCommitInfo(db, latestVer); err == nil && latestCommitInfo != nil {
			if len(latestCommitInfo.StoreInfos) > 0 {
				var storeKeys []string
				for _, storeInfo := range latestCommitInfo.StoreInfos {
					storeKeys = append(storeKeys, storeInfo.Name)
				}
				return storeKeys, nil
			}
		}
	}

	// Fallback: find any valid version
	logger.Debug("Latest version failed, scanning for any valid version...")
	validVersions, err := discoverValidAppVersions(db)
	if err != nil {
		return nil, fmt.Errorf("failed to discover valid versions: %w", err)
	}

	if len(validVersions) == 0 {
		return nil, fmt.Errorf("no valid versions found")
	}

	// Use the latest valid version to get store keys
	latestVer = validVersions[len(validVersions)-1]

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
