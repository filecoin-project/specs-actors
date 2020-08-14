package miner

import (
	"context"
	"github.com/filecoin-project/specs-actors/actors/runtime"
	orm2 "github.com/filecoin-project/specs-actors/support/orm"
	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
)

type MinerActor struct {
	MinerID         string `pg:",pk"`
	ParentStateRoot string `pg:",pk"`

	OwnerID    string
	WorkerID   string
	PeerID     string
	SectorSize string
}

type SectorPreCommitInfoModel struct {
	tableName struct{} `pg:"sector_pre_commit_info"`

	MinerID         string `pg:",pk"`
	ParentStateRoot string `pg:",pk"`

	SectorID           uint64 `pg:",use_zero"`
	SealedCID          string
	ExpirationEpoch    uint64 `pg:",use_zero"`
	PreCommitDeposit   string
	PreCommitEpoch     uint64 `pg:",use_zero"`
	DealWeight         string
	VerifiedDealWeight string

	IsReplaceCapacity      bool
	ReplaceSectorDeadline  uint64 `pg:",use_zero"`
	ReplaceSectorPartition uint64 `pg:",use_zero"`
	ReplaceSectorID        uint64 `pg:",use_zero"`

	PreCommitOnChainInfo *SectorPreCommitOnChainInfo `pg:"-"` // ignore this filed
}

func (s *SectorPreCommitInfoModel) BeforeInsert(ctx context.Context) (context.Context, error) {
	psr := orm2.CIDFromContext(ctx)
	s.ParentStateRoot = psr.String()
	s.MinerID = orm2.AddressFromContext(ctx)
	s.SectorID = uint64(s.PreCommitOnChainInfo.Info.SectorNumber)
	s.SealedCID = s.PreCommitOnChainInfo.Info.SealedCID.String()
	s.ExpirationEpoch = uint64(s.PreCommitOnChainInfo.Info.Expiration)
	s.PreCommitDeposit = s.PreCommitOnChainInfo.PreCommitDeposit.String()
	s.PreCommitEpoch = uint64(s.PreCommitOnChainInfo.PreCommitEpoch)
	s.DealWeight = s.PreCommitOnChainInfo.DealWeight.String()
	s.VerifiedDealWeight = s.PreCommitOnChainInfo.VerifiedDealWeight.String()
	s.IsReplaceCapacity = s.PreCommitOnChainInfo.Info.ReplaceCapacity
	s.ReplaceSectorDeadline = s.PreCommitOnChainInfo.Info.ReplaceSectorDeadline
	s.ReplaceSectorPartition = s.PreCommitOnChainInfo.Info.ReplaceSectorPartition
	s.ReplaceSectorID = uint64(s.PreCommitOnChainInfo.Info.ReplaceSectorNumber)
	return ctx, nil
}

type SectorInfo struct {
	MinerID         string `pg:",pk"`
	ParentStateRoot string `pg:",pk"`

	SectorID           uint64 `pg:",use_zero"`
	ActivationEpoch    uint64 `pg:",use_zero"`
	ExpirationEpoch    uint64 `pg:",use_zero"`
	DealWeight         string
	VerifiedDealWeight string
	InitialPledge      string

	SectorInfo *SectorOnChainInfo `pg:"-"` // ignore this filed
	Message    runtime.Message    `pg:"-"`
}

func (s *SectorInfo) BeforeInsert(ctx context.Context) (context.Context, error) {
	psr := orm2.CIDFromContext(ctx)
	s.ParentStateRoot = psr.String()
	s.MinerID = orm2.AddressFromContext(ctx)
	s.SectorID = uint64(s.SectorInfo.SectorNumber)
	s.ActivationEpoch = uint64(s.SectorInfo.Activation)
	s.ExpirationEpoch = uint64(s.SectorInfo.Expiration)
	s.DealWeight = s.SectorInfo.DealWeight.String()
	s.VerifiedDealWeight = s.SectorInfo.VerifiedDealWeight.String()
	s.InitialPledge = s.SectorInfo.InitialPledge.String()
	return ctx, nil
}

type MinerSectorEvents struct {
	MinerID         string `pg:",pk"`
	ParentStateRoot string `pg:",pk"`
	SectorID        uint64 `pg:",pk,use_zero"`
	Event           string `pg:",pk"`
}

func (s *MinerSectorEvents) BeforeInsert(ctx context.Context) (context.Context, error) {
	s.MinerID = orm2.AddressFromContext(ctx)
	s.ParentStateRoot = orm2.CIDFromContext(ctx).String()
	return ctx, nil
}

func CreateSchema(db *pg.DB) error {
	models := []interface{}{
		(*MinerActor)(nil),
		(*SectorPreCommitInfoModel)(nil),
		(*SectorInfo)(nil),
		(*MinerSectorEvents)(nil),
	}

	for _, model := range models {
		if err := db.Model(model).CreateTable(&orm.CreateTableOptions{
			IfNotExists: true,
		}); err != nil {
			return err
		}
	}
	return nil
}
