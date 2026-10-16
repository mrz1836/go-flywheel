// Package flywheel is a durable, transactional background-job runtime for
// Postgres and SQLite.
//
// This package is a thin, API-stable facade over the implementation in
// internal/core. Every exported symbol below is a type alias, a re-exported
// sentinel, or a forwarding function, so a consumer imports
// "github.com/mrz1836/go-flywheel" and calls flywheel.NewNode, flywheel.Insert,
// and the rest exactly as before, while the implementation lives behind an
// enforced internal boundary. The exported surface here is identical to the flat
// package it replaced.
package flywheel

import (
	"context"
	"time"

	core "github.com/mrz1836/go-flywheel/internal/core"
	node "github.com/mrz1836/go-flywheel/internal/node"
	"github.com/mrz1836/go-foundation/testutil"
	"gorm.io/gorm"
)

// Generic type aliases. Go 1.24+ generic aliases make a consumer's Job[A] and
// Worker[A] the same types as core's, so a Worker implementation satisfies the
// interface without change.

// Job is the typed job a Worker receives: its decoded Args plus row metadata.
type Job[A Args] = core.Job[A]

// Worker processes jobs whose args decode to A.
type Worker[A Args] = core.Worker[A]

// ArgsValidator is the optional worker interface that checks a kind's decoded
// args before Registry.Validate and Registry.Enqueue accept them.
type ArgsValidator[A Args] = core.ArgsValidator[A]

type (
	ActiveCount           = core.ActiveCount
	Actor                 = core.Actor
	ActorType             = core.ActorType
	AlreadyEnqueuedError  = core.AlreadyEnqueuedError
	Anomaly               = core.Anomaly
	AnomalyParams         = core.AnomalyParams
	AnomalySignal         = core.AnomalySignal
	AnomalyThresholds     = core.AnomalyThresholds
	Args                  = core.Args
	ArgsExampler          = core.ArgsExampler
	ArgsSchemer           = core.ArgsSchemer
	AuditAction           = core.AuditAction
	AuditEntry            = core.AuditEntry
	AuditHook             = core.AuditHook
	AuditOrder            = core.AuditOrder
	AuditPage             = core.AuditPage
	AuditTarget           = core.AuditTarget
	Barrier               = core.Barrier
	BarrierView           = core.BarrierView
	Baseline              = core.Baseline
	BatchItem             = core.BatchItem
	BatchOpts             = core.BatchOpts
	BatchProgress         = core.BatchProgress
	BatchResult           = core.BatchResult
	ChildOutput           = core.ChildOutput
	ClaimEvent            = core.ClaimEvent
	Classifier            = core.Classifier
	Client                = core.Client
	ClientOpts            = core.ClientOpts
	CountOpts             = core.CountOpts
	DBLimiter             = core.DBLimiter
	DBLimiterConfig       = core.DBLimiterConfig
	Defaults              = core.Defaults
	Describer             = core.Describer
	DrainTimeoutError     = core.DrainTimeoutError
	Driver                = core.Driver
	DriverOpts            = core.DriverOpts
	DurationSummary       = core.DurationSummary
	ErrorClass            = core.ErrorClass
	ErrorFingerprinter    = core.ErrorFingerprinter
	ErrorGroup            = core.ErrorGroup
	ErrorGroupsParams     = core.ErrorGroupsParams
	ErrorGroupsResult     = core.ErrorGroupsResult
	ExecutorClass         = core.ExecutorClass
	FailureView           = core.FailureView
	FakeHTTPDoer          = testutil.FakeHTTPDoer
	FinalizeOutcome       = core.FinalizeOutcome
	FinishEvent           = core.FinishEvent
	FinishedCursor        = core.FinishedCursor
	FollowUp              = core.FollowUp
	GetJobOpts            = core.GetJobOpts
	Grant                 = core.Grant
	HTTPDoer              = core.HTTPDoer
	HealthConfig          = node.HealthConfig
	Index                 = core.Index
	IndexDrift            = core.IndexDrift
	IndexDriftError       = core.IndexDriftError
	IndexKind             = core.IndexKind
	IndexOpts             = core.IndexOpts
	InsertHook            = core.InsertHook
	InsertInfo            = core.InsertInfo
	InsertOpts            = core.InsertOpts
	InsertSource          = core.InsertSource
	InspectIndexOpts      = core.InspectIndexOpts
	JobArgsView           = core.JobArgsView
	JobCount              = core.JobCount
	JobDetail             = core.JobDetail
	JobEvent              = core.JobEvent
	JobListItem           = core.JobListItem
	JobPage               = core.JobPage
	JobQuery              = core.JobQuery
	JobRunView            = core.JobRunView
	JobSort               = core.JobSort
	JobState              = core.JobState
	JobView               = core.JobView
	JobsOverview          = core.JobsOverview
	KindInfo              = core.KindInfo
	KindRecord            = core.KindRecord
	KindStats             = core.KindStats
	LeaseRenewal          = core.LeaseRenewal
	Limiter               = core.Limiter
	LimiterInfo           = core.LimiterInfo
	ListAuditParams       = core.ListAuditParams
	ListFinishedParams    = core.ListFinishedParams
	ListJobsParams        = core.ListJobsParams
	ListNodesParams       = core.ListNodesParams
	ListRunningParams     = core.ListRunningParams
	ListRunsParams        = core.ListRunsParams
	MigrateOpts           = core.MigrateOpts
	Node                  = node.Node
	NodeConfig            = node.NodeConfig
	NodeInfo              = core.NodeInfo
	NodeRunner            = core.NodeRunner
	Observer              = core.Observer
	OutcomeCounts         = core.OutcomeCounts
	OverviewParams        = core.OverviewParams
	PeriodicSpec          = core.PeriodicSpec
	PeriodicView          = core.PeriodicView
	PublishKindsOpts      = core.PublishKindsOpts
	QueueDepth            = core.QueueDepth
	QueueDepthPoint       = core.QueueDepthPoint
	QueueHealth           = core.QueueHealth
	QueueHistoryParams    = core.QueueHistoryParams
	QueueInfo             = core.QueueInfo
	RawJob                = core.RawJob
	RebuildOpts           = core.RebuildOpts
	RebuildResult         = core.RebuildResult
	RecentFailuresParams  = core.RecentFailuresParams
	Registry              = core.Registry
	ReplayOpts            = core.ReplayOpts
	Result                = core.Result
	RetentionOpts         = core.RetentionOpts
	RetryEvent            = core.RetryEvent
	RetryOpts             = core.RetryOpts
	Retryable             = core.Retryable
	RollupResult          = core.RollupResult
	RunOutcome            = core.RunOutcome
	RunSummary            = core.RunSummary
	RunSeed               = core.RunSeed
	Runner                = core.Runner
	RunnerConfig          = core.RunnerConfig
	RunningJob            = core.RunningJob
	SQLiteOpts            = core.SQLiteOpts
	Scheduler             = core.Scheduler
	SchedulerConfig       = core.SchedulerConfig
	SchemaDrift           = core.SchemaDrift
	ScopeOpts             = core.ScopeOpts
	ScopeResult           = core.ScopeResult
	SeriesInterval        = core.SeriesInterval
	SeriesParams          = core.SeriesParams
	SlowRunsParams        = core.SlowRunsParams
	StatsCoverage         = core.StatsCoverage
	StatsGroupBy          = core.StatsGroupBy
	StatsParams           = core.StatsParams
	StatsPoint            = core.StatsPoint
	StatsResult           = core.StatsResult
	StorageParameter      = core.StorageParameter
	StorageParameterDrift = core.StorageParameterDrift
	SupersedeEvent        = core.SupersedeEvent
	SweepEvent            = core.SweepEvent
	SweptJob              = core.SweptJob
	Timeouter             = core.Timeouter
	TokenBucket           = core.TokenBucket
	TokenBucketConfig     = core.TokenBucketConfig
	TruncatedOutput       = core.TruncatedOutput
	ValidationError       = core.ValidationError
	WaitOpts              = core.WaitOpts
)

const (
	ActorAgent               = core.ActorAgent
	ActorSystem              = core.ActorSystem
	ActorUser                = core.ActorUser
	AnyClass                 = core.AnyClass
	AuditActivated           = core.AuditActivated
	AuditCancelled           = core.AuditCancelled
	AuditDeactivated         = core.AuditDeactivated
	AuditDeleted             = core.AuditDeleted
	AuditDrainRequested      = core.AuditDrainRequested
	AuditNewest              = core.AuditNewest
	AuditOldest              = core.AuditOldest
	AuditPaused              = core.AuditPaused
	AuditReplayed            = core.AuditReplayed
	AuditReprioritized       = core.AuditReprioritized
	AuditRescheduled         = core.AuditRescheduled
	AuditResumed             = core.AuditResumed
	AuditRetried             = core.AuditRetried
	AuditTargetJob           = core.AuditTargetJob
	AuditTargetLimiter       = core.AuditTargetLimiter
	AuditTargetNode          = core.AuditTargetNode
	AuditTargetPeriodic      = core.AuditTargetPeriodic
	AuditTargetQueue         = core.AuditTargetQueue
	DefaultCountCap          = core.DefaultCountCap
	ErrorFingerprintVersion  = core.ErrorFingerprintVersion
	ErrorPermanent           = core.ErrorPermanent
	ErrorTimeout             = core.ErrorTimeout
	ErrorTransient           = core.ErrorTransient
	ErrorValidation          = core.ErrorValidation
	IndexCorrectness         = core.IndexCorrectness
	IndexPerformance         = core.IndexPerformance
	InsertSourceBarrier      = core.InsertSourceBarrier
	InsertSourceClient       = core.InsertSourceClient
	InsertSourceFollowUp     = core.InsertSourceFollowUp
	InsertSourcePeriodic     = core.InsertSourcePeriodic
	IntervalDay              = core.IntervalDay
	IntervalHour             = core.IntervalHour
	JobSortFinished          = core.JobSortFinished
	JobSortNewest            = core.JobSortNewest
	JobSortOldest            = core.JobSortOldest
	JobSortScheduled         = core.JobSortScheduled
	MaxActorSourceBytes      = core.MaxActorSourceBytes
	MaxContainmentWindow     = core.MaxContainmentWindow
	MaxCountCap              = core.MaxCountCap
	MaxHistoryPoints         = core.MaxHistoryPoints
	MaxMetadataBytes         = core.MaxMetadataBytes
	MaxReadLimit             = core.MaxReadLimit
	MaxReasonBytes           = core.MaxReasonBytes
	MaxTagBytes              = core.MaxTagBytes
	MaxTags                  = core.MaxTags
	OutcomeCancelled         = core.OutcomeCancelled
	OutcomeCrashed           = core.OutcomeCrashed
	OutcomeError             = core.OutcomeError
	OutcomeSnooze            = core.OutcomeSnooze
	OutcomeStarted           = core.OutcomeStarted
	OutcomeSuccess           = core.OutcomeSuccess
	OutcomeTimeout           = core.OutcomeTimeout
	SignalDurationRegression = core.SignalDurationRegression
	SignalFailureSpike       = core.SignalFailureSpike
	StateAvailable           = core.StateAvailable
	StateCancelled           = core.StateCancelled
	StateDiscarded           = core.StateDiscarded
	StatePaused              = core.StatePaused
	StateRetryable           = core.StateRetryable
	StateRunning             = core.StateRunning
	StateScheduled           = core.StateScheduled
	StateSucceeded           = core.StateSucceeded
	StatsByKind              = core.StatsByKind
	StatsByKindQueue         = core.StatsByKindQueue
	StatsByQueue             = core.StatsByQueue
)

var (
	ErrAlreadyEnqueued    = core.ErrAlreadyEnqueued
	ErrBarrierNoChildren  = core.ErrBarrierNoChildren
	ErrBarrierTooWide     = core.ErrBarrierTooWide
	ErrDecodeArgs         = core.ErrDecodeArgs
	ErrFollowUpLimit      = core.ErrFollowUpLimit
	ErrIndexDrift         = core.ErrIndexDrift
	ErrJobCancelled       = core.ErrJobCancelled
	ErrJobNotFound        = core.ErrJobNotFound
	ErrJobNotTerminal     = core.ErrJobNotTerminal
	ErrJobRunning         = core.ErrJobRunning
	ErrJobTerminal        = core.ErrJobTerminal
	ErrMissingKind        = core.ErrMissingKind
	ErrNodeNotFound       = core.ErrNodeNotFound
	ErrPeriodicNotFound   = core.ErrPeriodicNotFound
	ErrReplayUnbounded    = core.ErrReplayUnbounded
	ErrRunAlreadyRecorded = core.ErrRunAlreadyRecorded
	ErrRunnerStopped      = core.ErrRunnerStopped
	ErrSQLiteConcurrency  = core.ErrSQLiteConcurrency
	ErrSQLitePragma       = core.ErrSQLitePragma
	ErrSchemaOutdated     = core.ErrSchemaOutdated
	ErrStatsNotRolledUp   = core.ErrStatsNotRolledUp
	ErrUnknownKind        = core.ErrUnknownKind
	ErrUnsupportedDialect = core.ErrUnsupportedDialect
	ErrValidation         = core.ErrValidation
)

func ActorFrom(ctx context.Context) (Actor, bool) {
	return core.ActorFrom(ctx)
}

func Anomalies(ctx context.Context, db *gorm.DB, p AnomalyParams) ([]Anomaly, error) {
	return core.Anomalies(ctx, db, p)
}

func BackfillRunFinishes(ctx context.Context, db *gorm.DB) (int64, error) {
	return core.BackfillRunFinishes(ctx, db)
}

func Baselines(ctx context.Context, db *gorm.DB, window time.Duration) ([]Baseline, error) {
	return core.Baselines(ctx, db, window)
}

func CancelByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ScopeOpts) (ScopeResult, error) {
	return core.CancelByParent(ctx, db, parentJobID, opts)
}

func CancelJob(ctx context.Context, db *gorm.DB, id string) error {
	return core.CancelJob(ctx, db, id)
}

func ChildOutputs(ctx context.Context, db *gorm.DB, parentJobID string, p ListRunsParams) ([]ChildOutput, error) {
	return core.ChildOutputs(ctx, db, parentJobID, p)
}

func CountActiveByKind(ctx context.Context, db *gorm.DB) ([]ActiveCount, error) {
	return core.CountActiveByKind(ctx, db)
}

func CountActiveJobs(ctx context.Context, db *gorm.DB) (int64, error) {
	return core.CountActiveJobs(ctx, db)
}

func CountJobs(ctx context.Context, db *gorm.DB, q JobQuery) (JobCount, error) {
	return core.CountJobs(ctx, db, q)
}

func CountJobsWithOptions(ctx context.Context, db *gorm.DB, q JobQuery, opts CountOpts) (JobCount, error) {
	return core.CountJobsWithOptions(ctx, db, q, opts)
}

func CountRuns(ctx context.Context, db *gorm.DB) (int64, error) {
	return core.CountRuns(ctx, db)
}

func DefaultHTTPDoer() HTTPDoer {
	return core.DefaultHTTPDoer()
}

func DeleteAuditEntries(ctx context.Context, db *gorm.DB, olderThan time.Time) (int64, error) {
	return core.DeleteAuditEntries(ctx, db, olderThan)
}

func DeleteAuditEntriesWithOptions(ctx context.Context, db *gorm.DB, olderThan time.Time, opts RetentionOpts) (int64, error) {
	return core.DeleteAuditEntriesWithOptions(ctx, db, olderThan, opts)
}

func DeleteFinishedJobs(ctx context.Context, db *gorm.DB, olderThan time.Time) (int64, error) {
	return core.DeleteFinishedJobs(ctx, db, olderThan)
}

func DeleteFinishedJobsWithOptions(ctx context.Context, db *gorm.DB, olderThan time.Time, opts RetentionOpts) (int64, error) {
	return core.DeleteFinishedJobsWithOptions(ctx, db, olderThan, opts)
}

func DeleteJob(ctx context.Context, db *gorm.DB, id string) error {
	return core.DeleteJob(ctx, db, id)
}

func DeletePeriodic(ctx context.Context, db *gorm.DB, slug string) error {
	return core.DeletePeriodic(ctx, db, slug)
}

func Enqueue(ctx context.Context, c *Client, kind string, args []byte, opts InsertOpts) (string, error) {
	return core.Enqueue(ctx, c, kind, args, opts)
}

func ErrorFingerprint(kind string, class ErrorClass, message string) string {
	return core.ErrorFingerprint(kind, class, message)
}

func ErrorGroups(ctx context.Context, db *gorm.DB, p ErrorGroupsParams) (ErrorGroupsResult, error) {
	return core.ErrorGroups(ctx, db, p)
}

func ExplorerIndexSet(dialect string) ([]Index, error) {
	return core.ExplorerIndexSet(dialect)
}

func FindJob(ctx context.Context, db *gorm.DB, id string) (JobView, error) {
	return core.FindJob(ctx, db, id)
}

func GetJob(ctx context.Context, db *gorm.DB, id string) (JobDetail, error) {
	return core.GetJob(ctx, db, id)
}

func GetJobWithOptions(ctx context.Context, db *gorm.DB, id string, opts GetJobOpts) (JobDetail, error) {
	return core.GetJobWithOptions(ctx, db, id, opts)
}

func HistogramBounds(version int) []int64 {
	return core.HistogramBounds(version)
}

func IndexSet(dialect string) ([]Index, error) {
	return core.IndexSet(dialect)
}

func Indexes(dialect string) ([]string, error) {
	return core.Indexes(dialect)
}

func InsertMany(ctx context.Context, c *Client, items []BatchItem, opts BatchOpts) (BatchResult, error) {
	return core.InsertMany(ctx, c, items, opts)
}

func InspectIndexes(ctx context.Context, db *gorm.DB) ([]IndexDrift, error) {
	return core.InspectIndexes(ctx, db)
}

func InspectIndexesWithOptions(ctx context.Context, db *gorm.DB, opts InspectIndexOpts) ([]IndexDrift, error) {
	return core.InspectIndexesWithOptions(ctx, db, opts)
}

func InspectSchema(ctx context.Context, db *gorm.DB) ([]SchemaDrift, error) {
	return core.InspectSchema(ctx, db)
}

func InspectStorageParameters(ctx context.Context, db *gorm.DB) ([]StorageParameterDrift, error) {
	return core.InspectStorageParameters(ctx, db)
}

func InstallIndexes(ctx context.Context, db *gorm.DB) error {
	return core.InstallIndexes(ctx, db)
}

func InstallIndexesWithOptions(ctx context.Context, db *gorm.DB, opts IndexOpts) error {
	return core.InstallIndexesWithOptions(ctx, db, opts)
}

func InstallStorageParameters(ctx context.Context, db *gorm.DB) error {
	return core.InstallStorageParameters(ctx, db)
}

func LatestRun(ctx context.Context, db *gorm.DB, jobID string) (JobRunView, bool, error) {
	return core.LatestRun(ctx, db, jobID)
}

// ListActiveByKind returns the non-terminal jobs of a kind, each with its args.
//
// Deprecated: ListActiveByKind is unbounded by contract and returns args; use
// InsertOpts.UniqueActiveKey, which answers the same question with one index
// probe.
func ListActiveByKind(ctx context.Context, db *gorm.DB, kind string) ([]JobArgsView, error) {
	return core.ListActiveByKind(ctx, db, kind) //nolint:staticcheck // forwards a deprecated function under its own deprecation
}

func ListAudit(ctx context.Context, db *gorm.DB, p ListAuditParams) (AuditPage, error) {
	return core.ListAudit(ctx, db, p)
}

func ListFinished(ctx context.Context, db *gorm.DB, p ListFinishedParams) ([]JobView, error) {
	return core.ListFinished(ctx, db, p)
}

func ListJobs(ctx context.Context, db *gorm.DB, p ListJobsParams) ([]JobView, error) {
	return core.ListJobs(ctx, db, p)
}

func ListKinds(ctx context.Context, db *gorm.DB) ([]KindRecord, error) {
	return core.ListKinds(ctx, db)
}

func ListLimiters(ctx context.Context, db *gorm.DB) ([]LimiterInfo, error) {
	return core.ListLimiters(ctx, db)
}

func ListNodes(ctx context.Context, db *gorm.DB, p ListNodesParams) ([]NodeInfo, error) {
	return core.ListNodes(ctx, db, p)
}

func ListPeriodics(ctx context.Context, db *gorm.DB) ([]PeriodicView, error) {
	return core.ListPeriodics(ctx, db)
}

func ListQueues(ctx context.Context, db *gorm.DB) ([]QueueInfo, error) {
	return core.ListQueues(ctx, db)
}

func ListRunning(ctx context.Context, db *gorm.DB, p ListRunningParams) ([]RunningJob, error) {
	return core.ListRunning(ctx, db, p)
}

func ListRuns(ctx context.Context, db *gorm.DB, jobID string, p ListRunsParams) ([]JobRunView, error) {
	return core.ListRuns(ctx, db, jobID, p)
}

func Migrate(db *gorm.DB) error {
	return core.Migrate(db)
}

func MigrateWithOptions(db *gorm.DB, opts MigrateOpts) error {
	return core.MigrateWithOptions(db, opts)
}

func Models() []any {
	return core.Models()
}

func NewClient(writeDB *gorm.DB) *Client {
	return core.NewClient(writeDB)
}

func NewClientWithOptions(writeDB *gorm.DB, opts ClientOpts) *Client {
	return core.NewClientWithOptions(writeDB, opts)
}

func NewDBLimiter(db *gorm.DB, cfg DBLimiterConfig) (*DBLimiter, error) {
	return core.NewDBLimiter(db, cfg)
}

func NewFakeHTTPDoer() *FakeHTTPDoer {
	return testutil.NewFakeHTTPDoer()
}

func NewNode(cfg NodeConfig) (*Node, error) {
	return node.NewNode(cfg)
}

func NewPostgresDriver(db *gorm.DB) Driver {
	return core.NewPostgresDriver(db)
}

func NewPostgresDriverWithOptions(db *gorm.DB, opts DriverOpts) Driver {
	return core.NewPostgresDriverWithOptions(db, opts)
}

func NewRegistry() *Registry {
	return core.NewRegistry()
}

func NewRunner(cfg RunnerConfig) (*Runner, error) {
	return core.NewRunner(cfg)
}

func NewSQLiteDriver(db *gorm.DB) Driver {
	return core.NewSQLiteDriver(db)
}

func NewSQLiteDriverWithOptions(db *gorm.DB, opts SQLiteOpts) (Driver, error) {
	return core.NewSQLiteDriverWithOptions(db, opts)
}

func NewScheduler(db *gorm.DB, client *Client) (*Scheduler, error) {
	return core.NewScheduler(db, client)
}

func NewSchedulerWithConfig(cfg SchedulerConfig) (*Scheduler, error) {
	return core.NewSchedulerWithConfig(cfg)
}

func NewTokenBucket(cfg TokenBucketConfig) *TokenBucket {
	return core.NewTokenBucket(cfg)
}

func NonTerminalStates() []JobState {
	return core.NonTerminalStates()
}

func NormalizeErrorMessage(message string) string {
	return core.NormalizeErrorMessage(message)
}

func NormalizeRunTimestamps(ctx context.Context, db *gorm.DB) (int64, error) {
	return core.NormalizeRunTimestamps(ctx, db)
}

func Overview(ctx context.Context, db *gorm.DB, p OverviewParams) (JobsOverview, error) {
	return core.Overview(ctx, db, p)
}

func PauseByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ScopeOpts) (ScopeResult, error) {
	return core.PauseByParent(ctx, db, parentJobID, opts)
}

func PauseJob(ctx context.Context, db *gorm.DB, id string) error {
	return core.PauseJob(ctx, db, id)
}

func PausePeriodic(ctx context.Context, db *gorm.DB, slug string) error {
	return core.PausePeriodic(ctx, db, slug)
}

func PauseQueue(ctx context.Context, db *gorm.DB, queue string) error {
	return core.PauseQueue(ctx, db, queue)
}

func Progress(ctx context.Context, db *gorm.DB, parentJobID string) (BatchProgress, error) {
	return core.Progress(ctx, db, parentJobID)
}

func ProgressByKind(ctx context.Context, db *gorm.DB, kinds []string) (map[string]BatchProgress, error) {
	return core.ProgressByKind(ctx, db, kinds)
}

func ProgressMany(ctx context.Context, db *gorm.DB, parentJobIDs []string) (map[string]BatchProgress, error) {
	return core.ProgressMany(ctx, db, parentJobIDs)
}

func PublishKinds(ctx context.Context, db *gorm.DB, reg *Registry) error {
	return core.PublishKinds(ctx, db, reg)
}

func PublishKindsWithOptions(ctx context.Context, db *gorm.DB, reg *Registry, opts PublishKindsOpts) error {
	return core.PublishKindsWithOptions(ctx, db, reg, opts)
}

func QueryJobs(ctx context.Context, db *gorm.DB, q JobQuery) (JobPage, error) {
	return core.QueryJobs(ctx, db, q)
}

func QueueDepthHistory(ctx context.Context, db *gorm.DB, p QueueHistoryParams) ([]QueueDepthPoint, error) {
	return core.QueueDepthHistory(ctx, db, p)
}

func QueueDepths(ctx context.Context, db *gorm.DB) ([]QueueDepth, error) {
	return core.QueueDepths(ctx, db)
}

func RebuildStats(ctx context.Context, db *gorm.DB, opts RebuildOpts) (RebuildResult, error) {
	return core.RebuildStats(ctx, db, opts)
}

func RecentFailures(ctx context.Context, db *gorm.DB, p RecentFailuresParams) ([]FailureView, error) {
	return core.RecentFailures(ctx, db, p)
}

func Replay(ctx context.Context, db *gorm.DB, opts ReplayOpts) (ScopeResult, error) {
	return core.Replay(ctx, db, opts)
}

func ReplayByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ReplayOpts) (ScopeResult, error) {
	return core.ReplayByParent(ctx, db, parentJobID, opts)
}

func RequestDrain(ctx context.Context, db *gorm.DB, nodeID string) error {
	return core.RequestDrain(ctx, db, nodeID)
}

func RescheduleJob(ctx context.Context, db *gorm.DB, id string, at time.Time) error {
	return core.RescheduleJob(ctx, db, id, at)
}

func ResumeByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ScopeOpts) (ScopeResult, error) {
	return core.ResumeByParent(ctx, db, parentJobID, opts)
}

func ResumeJob(ctx context.Context, db *gorm.DB, id string) error {
	return core.ResumeJob(ctx, db, id)
}

func ResumePeriodic(ctx context.Context, db *gorm.DB, slug string) error {
	return core.ResumePeriodic(ctx, db, slug)
}

func ResumeQueue(ctx context.Context, db *gorm.DB, queue string) error {
	return core.ResumeQueue(ctx, db, queue)
}

func RetryJob(ctx context.Context, db *gorm.DB, id string) error {
	return core.RetryJob(ctx, db, id)
}

func RetryJobWithOptions(ctx context.Context, db *gorm.DB, id string, opts RetryOpts) error {
	return core.RetryJobWithOptions(ctx, db, id, opts)
}

func SampleQueueHealth(ctx context.Context, db *gorm.DB) (QueueHealth, error) {
	return core.SampleQueueHealth(ctx, db)
}

func SeedRun(ctx context.Context, db *gorm.DB, seed RunSeed) (string, error) {
	return core.SeedRun(ctx, db, seed)
}

func SetJobPriority(ctx context.Context, db *gorm.DB, id string, priority int) error {
	return core.SetJobPriority(ctx, db, id, priority)
}

func SetPeriodicActive(ctx context.Context, db *gorm.DB, slug string, active bool) error {
	return core.SetPeriodicActive(ctx, db, slug, active)
}

func SlowRuns(ctx context.Context, db *gorm.DB, p SlowRunsParams) ([]JobRunView, error) {
	return core.SlowRuns(ctx, db, p)
}

func Stats(ctx context.Context, db *gorm.DB, p StatsParams) (StatsResult, error) {
	return core.Stats(ctx, db, p)
}

func StatsSeries(ctx context.Context, db *gorm.DB, p SeriesParams) ([]StatsPoint, error) {
	return core.StatsSeries(ctx, db, p)
}

func StorageParameterSet(dialect string) ([]StorageParameter, error) {
	return core.StorageParameterSet(dialect)
}

func StorageParameters(dialect string) ([]string, error) {
	return core.StorageParameters(dialect)
}

func TerminalStates() []JobState {
	return core.TerminalStates()
}

func UpsertPeriodic(ctx context.Context, db *gorm.DB, spec PeriodicSpec) error {
	return core.UpsertPeriodic(ctx, db, spec)
}

func WaitForJob(ctx context.Context, db *gorm.DB, id string) (JobView, error) {
	return core.WaitForJob(ctx, db, id)
}

func WaitForJobWithOptions(ctx context.Context, db *gorm.DB, id string, opts WaitOpts) (JobView, error) {
	return core.WaitForJobWithOptions(ctx, db, id, opts)
}

func WithActor(ctx context.Context, a Actor) context.Context {
	return core.WithActor(ctx, a)
}

func WithAuditHook(ctx context.Context, hook AuditHook) context.Context {
	return core.WithAuditHook(ctx, hook)
}

// Generic function forwarders. A generic function cannot be an alias, so each
// forwards to core with its type argument threaded through.

// Insert enqueues one typed job and returns its id.
func Insert[A Args](ctx context.Context, c *Client, args A, opts InsertOpts) (string, error) {
	return core.Insert[A](ctx, c, args, opts)
}

// InsertManyTyped enqueues a homogeneous batch of typed jobs.
func InsertManyTyped[A Args](ctx context.Context, c *Client, args []A, opts InsertOpts, batch BatchOpts) (BatchResult, error) {
	return core.InsertManyTyped[A](ctx, c, args, opts, batch)
}

// Register wires a typed Worker into a Registry.
func Register[A Args](reg *Registry, w Worker[A]) {
	core.Register[A](reg, w)
}
