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

type (
	Args                  = core.Args
	Barrier               = core.Barrier
	BatchItem             = core.BatchItem
	BatchOpts             = core.BatchOpts
	BatchProgress         = core.BatchProgress
	BatchResult           = core.BatchResult
	ChildOutput           = core.ChildOutput
	ClaimEvent            = core.ClaimEvent
	Classifier            = core.Classifier
	Client                = core.Client
	DBLimiter             = core.DBLimiter
	DBLimiterConfig       = core.DBLimiterConfig
	Defaults              = core.Defaults
	DrainTimeoutError     = core.DrainTimeoutError
	Driver                = core.Driver
	DriverOpts            = core.DriverOpts
	ErrorClass            = core.ErrorClass
	ExecutorClass         = core.ExecutorClass
	FailureView           = core.FailureView
	FakeHTTPDoer          = testutil.FakeHTTPDoer
	FinalizeOutcome       = core.FinalizeOutcome
	FinishEvent           = core.FinishEvent
	FollowUp              = core.FollowUp
	Grant                 = core.Grant
	HTTPDoer              = core.HTTPDoer
	HealthConfig          = node.HealthConfig
	Index                 = core.Index
	IndexDrift            = core.IndexDrift
	IndexDriftError       = core.IndexDriftError
	IndexKind             = core.IndexKind
	IndexOpts             = core.IndexOpts
	InsertOpts            = core.InsertOpts
	JobArgsView           = core.JobArgsView
	JobEvent              = core.JobEvent
	JobRunView            = core.JobRunView
	JobState              = core.JobState
	JobView               = core.JobView
	JobsOverview          = core.JobsOverview
	LeaseRenewal          = core.LeaseRenewal
	Limiter               = core.Limiter
	ListJobsParams        = core.ListJobsParams
	ListRunsParams        = core.ListRunsParams
	MigrateOpts           = core.MigrateOpts
	Node                  = node.Node
	NodeConfig            = node.NodeConfig
	Observer              = core.Observer
	OverviewParams        = core.OverviewParams
	PeriodicSpec          = core.PeriodicSpec
	PeriodicView          = core.PeriodicView
	QueueHealth           = core.QueueHealth
	RawJob                = core.RawJob
	RecentFailuresParams  = core.RecentFailuresParams
	Registry              = core.Registry
	ReplayOpts            = core.ReplayOpts
	Result                = core.Result
	RetentionOpts         = core.RetentionOpts
	RetryEvent            = core.RetryEvent
	RetryOpts             = core.RetryOpts
	Retryable             = core.Retryable
	RunOutcome            = core.RunOutcome
	RunSeed               = core.RunSeed
	Runner                = core.Runner
	RunnerConfig          = core.RunnerConfig
	SQLiteOpts            = core.SQLiteOpts
	Scheduler             = core.Scheduler
	SchedulerConfig       = core.SchedulerConfig
	ScopeOpts             = core.ScopeOpts
	ScopeResult           = core.ScopeResult
	StorageParameter      = core.StorageParameter
	StorageParameterDrift = core.StorageParameterDrift
	SupersedeEvent        = core.SupersedeEvent
	SweepEvent            = core.SweepEvent
	Timeouter             = core.Timeouter
	TokenBucket           = core.TokenBucket
	TokenBucketConfig     = core.TokenBucketConfig
	ValidationError       = core.ValidationError
)

const (
	AnyClass         = core.AnyClass
	ErrorPermanent   = core.ErrorPermanent
	ErrorTimeout     = core.ErrorTimeout
	ErrorTransient   = core.ErrorTransient
	ErrorValidation  = core.ErrorValidation
	IndexCorrectness = core.IndexCorrectness
	IndexPerformance = core.IndexPerformance
	OutcomeCancelled = core.OutcomeCancelled
	OutcomeCrashed   = core.OutcomeCrashed
	OutcomeError     = core.OutcomeError
	OutcomeSnooze    = core.OutcomeSnooze
	OutcomeStarted   = core.OutcomeStarted
	OutcomeSuccess   = core.OutcomeSuccess
	OutcomeTimeout   = core.OutcomeTimeout
	StateAvailable   = core.StateAvailable
	StateCancelled   = core.StateCancelled
	StateDiscarded   = core.StateDiscarded
	StatePaused      = core.StatePaused
	StateRetryable   = core.StateRetryable
	StateRunning     = core.StateRunning
	StateScheduled   = core.StateScheduled
	StateSucceeded   = core.StateSucceeded
)

var (
	ErrAlreadyEnqueued    = core.ErrAlreadyEnqueued
	ErrBarrierNoChildren  = core.ErrBarrierNoChildren
	ErrBarrierTooWide     = core.ErrBarrierTooWide
	ErrFollowUpLimit      = core.ErrFollowUpLimit
	ErrIndexDrift         = core.ErrIndexDrift
	ErrJobNotFound        = core.ErrJobNotFound
	ErrJobTerminal        = core.ErrJobTerminal
	ErrMissingKind        = core.ErrMissingKind
	ErrPeriodicNotFound   = core.ErrPeriodicNotFound
	ErrReplayUnbounded    = core.ErrReplayUnbounded
	ErrRunAlreadyRecorded = core.ErrRunAlreadyRecorded
	ErrRunnerStopped      = core.ErrRunnerStopped
	ErrSQLiteConcurrency  = core.ErrSQLiteConcurrency
	ErrSQLitePragma       = core.ErrSQLitePragma
	ErrUnknownKind        = core.ErrUnknownKind
	ErrUnsupportedDialect = core.ErrUnsupportedDialect
	ErrValidation         = core.ErrValidation
)

func CancelByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ScopeOpts) (ScopeResult, error) {
	return core.CancelByParent(ctx, db, parentJobID, opts)
}

func CancelJob(ctx context.Context, db *gorm.DB, id string) error {
	return core.CancelJob(ctx, db, id)
}

func ChildOutputs(ctx context.Context, db *gorm.DB, parentJobID string, p ListRunsParams) ([]ChildOutput, error) {
	return core.ChildOutputs(ctx, db, parentJobID, p)
}

func CountActiveJobs(ctx context.Context, db *gorm.DB) (int64, error) {
	return core.CountActiveJobs(ctx, db)
}

func CountRuns(ctx context.Context, db *gorm.DB) (int64, error) {
	return core.CountRuns(ctx, db)
}

func DefaultHTTPDoer() HTTPDoer {
	return core.DefaultHTTPDoer()
}

func DeleteFinishedJobs(ctx context.Context, db *gorm.DB, olderThan time.Time) (int64, error) {
	return core.DeleteFinishedJobs(ctx, db, olderThan)
}

func DeleteFinishedJobsWithOptions(ctx context.Context, db *gorm.DB, olderThan time.Time, opts RetentionOpts) (int64, error) {
	return core.DeleteFinishedJobsWithOptions(ctx, db, olderThan, opts)
}

func DeletePeriodic(ctx context.Context, db *gorm.DB, slug string) error {
	return core.DeletePeriodic(ctx, db, slug)
}

func Enqueue(ctx context.Context, c *Client, kind string, args []byte, opts InsertOpts) (string, error) {
	return core.Enqueue(ctx, c, kind, args, opts)
}

func FindJob(ctx context.Context, db *gorm.DB, id string) (JobView, error) {
	return core.FindJob(ctx, db, id)
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

func ListActiveByKind(ctx context.Context, db *gorm.DB, kind string) ([]JobArgsView, error) {
	return core.ListActiveByKind(ctx, db, kind)
}

func ListJobs(ctx context.Context, db *gorm.DB, p ListJobsParams) ([]JobView, error) {
	return core.ListJobs(ctx, db, p)
}

func ListPeriodics(ctx context.Context, db *gorm.DB) ([]PeriodicView, error) {
	return core.ListPeriodics(ctx, db)
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

func Overview(ctx context.Context, db *gorm.DB, p OverviewParams) (JobsOverview, error) {
	return core.Overview(ctx, db, p)
}

func PauseByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ScopeOpts) (ScopeResult, error) {
	return core.PauseByParent(ctx, db, parentJobID, opts)
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

func RecentFailures(ctx context.Context, db *gorm.DB, p RecentFailuresParams) ([]FailureView, error) {
	return core.RecentFailures(ctx, db, p)
}

func Replay(ctx context.Context, db *gorm.DB, opts ReplayOpts) (ScopeResult, error) {
	return core.Replay(ctx, db, opts)
}

func ReplayByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ReplayOpts) (ScopeResult, error) {
	return core.ReplayByParent(ctx, db, parentJobID, opts)
}

func ResumeByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ScopeOpts) (ScopeResult, error) {
	return core.ResumeByParent(ctx, db, parentJobID, opts)
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

func SetPeriodicActive(ctx context.Context, db *gorm.DB, slug string, active bool) error {
	return core.SetPeriodicActive(ctx, db, slug, active)
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
