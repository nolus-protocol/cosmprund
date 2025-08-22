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

// load db
// load app store and prune
// if immutable tree is not deletable we should import and export current state

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
	appDB, errDB := openDB("application", home)
	if errDB != nil {
		return errDB
	}

	defer appDB.Close()

	var err error

	logger.Info("pruning application state")

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

	// TODO: cleanup app state
	appStore := rootmulti.NewStore(appDB)

	for _, value := range keys {
		appStore.MountStoreWithDB(storetypes.NewKVStoreKey(value), sdk.StoreTypeIAVL, nil)
	}

	err = appStore.LoadLatestVersion()
	if err != nil {
		return fmt.Errorf("failed to load latest version: %w", err)
	}

	// Use the application store's actual latest version for tx_index height
	if txIdxHeight <= 0 {
		txIdxHeight = appStore.LastCommitID().Version
		logger.Debug("set txIdxHeight=%d from application store", txIdxHeight)
	}

	appLatestVersion := appStore.LastCommitID().Version
	logger.Debug("application store latest version: %d", appLatestVersion)

	allVersions := appStore.GetAllVersions()

	v64 := make([]int64, len(allVersions))
	for i := 0; i < len(allVersions); i++ {
		v64[i] = int64(allVersions[i])
	}

	logger.Debug("total versions found: %d", len(v64))
	versionsToPrune := int64(len(v64)) - int64(versions)
	logger.Debug("versionsToPrune=%d", versionsToPrune)
	if versionsToPrune <= 0 {
		logger.Info("No need to prune app state (versionsToPrune=%d)", versionsToPrune)
	} else {
		appStore.PruneHeights = v64[:versionsToPrune]
		appStore.PruneStores()
	}

	if compact {
		logger.Info("compacting application state")
		if err := compactDB(appDB); err != nil {
			logger.Error("Failed to compact application state: %v", err)
		}
	}

	return nil
}

// pruneTMData prunes the tendermint blocks and state based on the amount of blocks to keep
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

	var err error

	stateStore := state.NewStore(stateDB)
	defer stateStore.Close()

	base := blockStore.Base()

	pruneHeight := blockStore.Height() - int64(blocks)
	logger.Debug("pruneHeight=%d", pruneHeight)
	if pruneHeight <= 0 {
		logger.Info("No need to prune tendermint data")
		return nil
	}

	if txIdxHeight <= 0 {
		txIdxHeight = blockStore.Height()
		logger.Debug("set txIdxHeight=%d", txIdxHeight)
	}

	logger.Info("pruning block store")

	// prune block store
	// prune in batches to avoid memory issues and maintain performance with LevelDB
	for pruneBlockFrom := base; pruneBlockFrom < pruneHeight-1; pruneBlockFrom += rootmulti.PRUNE_BATCH_SIZE {
		height := pruneBlockFrom
		if height >= pruneHeight-1 {
			height = pruneHeight - 1
		}

		_, err = blockStore.PruneBlocks(height)
		if err != nil {
			//return err
			logger.Error("Failed to prune blocks at height %d: %v", height, err)
		}
	}

	if compact {
		logger.Info("compacting block store")
		if err := compactDB(blockStoreDB); err != nil {
			logger.Error("Failed to compact block store: %v", err)
		}
	}

	// CRITICAL: Skip state store pruning to avoid corruption
	logger.Warn("Skipping state store pruning due to consensus data corruption risk")
	logger.Info("State store contains critical validator and consensus data needed for node operation")
	logger.Info("Consider using node's built-in pruning options instead for state data")

	// DISABLED STATE STORE PRUNING - Too dangerous
	/*
	logger.Info("pruning state store")
	// prune state store
	// prune in batches to avoid memory issues and maintain performance with LevelDB
	for pruneStateFrom := base; pruneStateFrom < pruneHeight-1; pruneStateFrom += rootmulti.PRUNE_BATCH_SIZE {
		endHeight := pruneStateFrom + rootmulti.PRUNE_BATCH_SIZE
		if endHeight >= pruneHeight-1 {
			endHeight = pruneHeight - 1
		}
		err = stateStore.PruneStates(pruneStateFrom, endHeight)
		if err != nil {
			//return err
			logger.Error("Failed to prune states from %d to %d: %v", pruneStateFrom, endHeight, err)
		}
	}

	if compact {
		logger.Info("compacting state store")
		if err := compactDB(stateDB); err != nil {
			logger.Error("Failed to compact state store: %v", err)
		}
	}
	*/

	return nil
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
	latestVer := rootmulti.GetLatestVersion(db)
	if latestVer <= 0 {
		return nil, fmt.Errorf("no latest version found (version=%d)", latestVer)
	}

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
