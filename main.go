package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"gitlab.com/NebulousLabs/bolt"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
)

type (
	// storageObligationStatus indicates the current status of a storage obligation
	storageObligationStatus uint64

	// storageObligation contains all of the metadata related to a file contract
	// and the storage contained by the file contract.
	storageObligation struct {
		// Storage obligations are broken up into ordered atomic sectors that are
		// exactly 4MiB each. By saving the roots of each sector, storage proofs
		// and modifications to the data can be made inexpensively by making use of
		// the merkletree.CachedTree. Sectors can be appended, modified, or deleted
		// and the host can recompute the Merkle root of the whole file without
		// much computational or I/O expense.
		SectorRoots []crypto.Hash

		// Variables about the file contract that enforces the storage obligation.
		// The origin an revision transaction are stored as a set, where the set
		// contains potentially unconfirmed transactions.
		ContractCost             types.Currency
		LockedCollateral         types.Currency
		PotentialAccountFunding  types.Currency
		PotentialDownloadRevenue types.Currency
		PotentialStorageRevenue  types.Currency
		PotentialUploadRevenue   types.Currency
		RiskedCollateral         types.Currency
		TransactionFeesAdded     types.Currency

		// The negotiation height specifies the block height at which the file
		// contract was negotiated. If the origin transaction set is not accepted
		// onto the blockchain quickly enough, the contract is pruned from the
		// host. The origin and revision transaction set contain the contracts +
		// revisions as well as all parent transactions. The parents are necessary
		// because after a restart the transaction pool may be emptied out.
		NegotiationHeight      types.BlockHeight
		OriginTransactionSet   []types.Transaction
		RevisionTransactionSet []types.Transaction

		// Variables indicating whether the critical transactions in a storage
		// obligation have been confirmed on the blockchain.
		ObligationStatus    storageObligationStatus
		OriginConfirmed     bool
		ProofConfirmed      bool
		ProofConstructed    bool
		RevisionConfirmed   bool
		RevisionConstructed bool
	}
)

var (
	// bucketStorageObligations contains a set of serialized
	// 'storageObligations' sorted by their file contract id.
	bucketStorageObligations = []byte("BucketStorageObligations")
)

// String converts a storageObligationStatus to a string.
func (i storageObligationStatus) String() string {
	if i == 0 {
		return "obligationUnresolved"
	}
	if i == 1 {
		return "obligationRejected"
	}
	if i == 2 {
		return "obligationSucceeded"
	}
	if i == 3 {
		return "obligationFailed"
	}
	return "storageObligationStatus(" + strconv.FormatInt(int64(i), 10) + ")"
}

func main() {
	db, err := bolt.Open(os.Args[1], 0600, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	var corruptKeys [][]byte
	var corrupt, working int

	err = db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketStorageObligations).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var so storageObligation
			contractID := hex.EncodeToString(k)
			if err := json.Unmarshal(v, &so); err != nil {
				log.Printf("corrupt storage obligation %x: %v", k, err)
				corruptKeys = append(corruptKeys, k)
				// write the corrupted obligation
				err := func() error {
					f, err := os.Create(fmt.Sprintf("contract-%v-corrupted.json", contractID))
					if err != nil {
						return fmt.Errorf("failed to create file: %w", err)
					}
					defer f.Close()
					if _, err := f.Write(v); err != nil {
						return fmt.Errorf("failed to write data: %w", err)
					}
					return nil
				}()
				if err != nil {
					return fmt.Errorf("failed to write corrupted contract %v: %w", contractID, err)
				}
				corrupt++
				continue
			}
			working++
		}
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}

	// Uncomment to remove the corrupt obligations
	/*log.Printf("Removing %v/%v corrupt storage obligations", corrupt, working)
	err = db.Update(func(tx *bolt.Tx) error {
		for _, k := range corruptKeys {
			if err := tx.Bucket(bucketStorageObligations).Delete(k); err != nil {
				return fmt.Errorf("failed to delete corrupt obligation %x: %w", k, err)
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketStorageObligations).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var so storageObligation
			if err := json.Unmarshal(v, &so); err != nil {
				return fmt.Errorf("still corrupt: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}*/
}
