// Command app is the composition root of the workflow engine: it loads
// configuration, opens the database, seeds the system user, wires the HTTP
// router and the leased dispatcher, and coordinates graceful shutdown. It
// contains no workflow rules.
package main

import (
	// @title SimpWF API
	// @version 1.0
	// @description Workflow definition and execution API.
	// @BasePath /
	// @schemes http
	// @securityDefinitions.apikey ApiKeyAuth
	// @in header
	// @name X-Api-Token
	// @description API token authentication via the X-Api-Token header. The token authenticates as the service principal, which bypasses every authorization gate.
	// @securityDefinitions.apikey BearerAuth
	// @in header
	// @name Authorization
	// @description OIDC bearer token. In the Swagger Authorize dialog type "Bearer <jwt>" with the Bearer prefix and a space: the value is sent verbatim as the Authorization header. The engine is a resource server: the frontend completes a code+PKCE flow against the provider and calls the API with the JWT. The authorization URL, client id, and scopes are served by GET /v1/auth/config.
	// @type apiKey

	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/handler"
	"github.com/simpwf/workflow-engine/internal/workflow/inputtransport"
	"github.com/simpwf/workflow-engine/internal/workflow/kernel"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/scheduler"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/internal/workflow/statusupdate"
	"github.com/simpwf/workflow-engine/internal/workflow/transport"
	"github.com/simpwf/workflow-engine/pkg/configuration"
	_ "github.com/simpwf/workflow-engine/pkg/customnode/all"
	"github.com/simpwf/workflow-engine/pkg/database"
	"github.com/simpwf/workflow-engine/pkg/envsnapshot"
	"github.com/sirupsen/logrus"
)

const shutdownTimeout = 10 * time.Second

func main() {
	configPath := flag.String("config", "", "path to config file (default: config.yaml in working directory)")
	flag.Parse()

	logger, err := NewLogger("info")
	if err != nil {
		logrus.Fatal(err)
	}

	cfg, err := configuration.Load(configuration.WithConfigFile(*configPath))
	if err != nil {
		logger.Fatalf("load configuration: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Fatalf("run: %v", err)
	}
	logger.Info("app stopped cleanly")
}

// warnAboutCatalog reports configured role permissions that will never take
// effect. An unknown action is not rejected: the catalog is data, and a
// deployment may legitimately grant an action a newer build gates on. But a
// typo silently grants nothing, which is exactly the failure an operator
// cannot see from the outside, so it is worth a boot-time line. A role name
// that is blank once trimmed names nobody at all.
func warnAboutCatalog(rolePermissions map[string][]string, logger *logrus.Logger) {
	known := make(map[string]bool, len(auth.KnownActions()))
	for _, action := range auth.KnownActions() {
		known[action] = true
	}
	roles := make([]string, 0, len(rolePermissions))
	for role := range rolePermissions {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	for _, role := range roles {
		if strings.TrimSpace(role) == "" {
			logger.Warnf("auth: a configured role name is blank and grants nothing")
			continue
		}
		for _, action := range rolePermissions[role] {
			action = strings.TrimSpace(action)
			switch {
			case action == "":
				logger.Warnf("auth: role %q has a blank permission, which grants nothing", role)
			case !known[action] && action != auth.WildcardAction:
				logger.Warnf("auth: role %q grants unknown action %q; no route gates on it", role, action)
			}
		}
	}
}

// checkAuthWiring refuses to boot an authentication setup that cannot name
// the caller it just authenticated.
//
// OIDC resolves a verified token onto a users row just in time, keyed by
// (subject, issuer); the human rows get their id from that upsert, not from
// the configured actor. The dangerous case a blank audit actor creates is
// narrower but still silent: the API-token service principal resolves to an
// empty user id, as do the engine background effects that act as the
// configured actor, so their created_by/updated_by rows are written empty
// and the work is attributed to nobody. The failure is silent, so it is
// cheaper to refuse the boot. Without OIDC the same misconfiguration only
// affects the API-token service principal, which is optional, so it is left
// to the configured default.
func checkAuthWiring(verifier auth.Verifier, authSvc service.AuthService, actor string) error {
	if verifier == nil {
		return nil
	}
	if authSvc == nil {
		return errors.New("oidc enabled without an identity resolver: the auth service is required")
	}
	if strings.TrimSpace(actor) == "" {
		return errors.New("oidc enabled without a system user id: system.user_id is required")
	}
	return nil
}

// run starts the HTTP server and the dispatcher, and blocks until ctx is
// cancelled, then shuts down gracefully. It returns nil on a clean shutdown.
func run(ctx context.Context, cfg *configuration.Config, logger *logrus.Logger) error {
	// Env snapshot deny adjustments are global state read by every snapshot
	// and render, so they are installed once here before any workflow runs.
	envsnapshot.SetOverride(cfg.Engine.EnvDenyExtra, cfg.Engine.EnvAllowExceptions)

	db, err := database.New(database.Options{DSN: cfg.Infra.PostgreSQL.DSN})
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("database pool: %w", err)
	}
	defer func() { _ = sqlDB.Close() }()
	logger.Info("database connected")

	actor := cfg.System.UserID
	if actor == "" {
		actor = "11111111-1111-7111-8111-111111111111"
	}
	sysUser := model.User{ID: actor, Name: cfg.System.Name, Email: cfg.System.Email}
	if sysUser.Name == "" {
		sysUser.Name = "system"
	}
	if sysUser.Email == "" {
		sysUser.Email = "system@localhost"
	}
	if err := repository.UpsertSystemUser(ctx, db, sysUser); err != nil {
		return fmt.Errorf("seed system user: %w", err)
	}

	nodeDefs := repository.NewNodeDefinitionRepository(db)
	wfDefs := repository.NewWorkflowDefinitionRepository(db)
	secrets := repository.NewSecretRepository(db)
	users := repository.NewUserRepository(db)
	roles := repository.NewRoleRepository(db)

	// Authentication is configured, not discovered: the OIDC provider is
	// contacted at startup so a wrong issuer or client id fails the boot
	// rather than every request. With OIDC off, the nil verifier leaves the
	// API-token service principal (or an unauthenticated deployment) as-is.
	var oidcVerifier auth.Verifier
	if cfg.Auth.OIDC.Enabled {
		oidcAuth, err := auth.NewOIDCAuthenticator(ctx, cfg.Auth.OIDC)
		if err != nil {
			return fmt.Errorf("oidc: %w", err)
		}
		oidcVerifier = oidcAuth
		logger.Infof("oidc enabled for issuer %s", cfg.Auth.OIDC.Issuer)
	}
	catalog := auth.CatalogFromConfig(cfg.Auth)
	warnAboutCatalog(cfg.Auth.RolePermissions, logger)
	authSvc := service.NewAuthService(users, roles, catalog, actor)
	if err := checkAuthWiring(oidcVerifier, authSvc, actor); err != nil {
		return err
	}
	// The catalog is upserted and its stale permissions pruned, but no role
	// row is ever deleted: a role dropped from the config keeps its rows so
	// an in-flight token still resolves it.
	if err := authSvc.SeedRoles(ctx); err != nil {
		return fmt.Errorf("seed roles: %w", err)
	}

	leanOpts := model.LeanOptions{
		LeanContextDefault: cfg.Engine.LeanContextDefault,
		AnchorEvery:        cfg.Engine.LeanAnchorEvery,
		ReplayMax:          cfg.Engine.LeanReplayMax,
	}
	instances := repository.NewInstanceRepositoryWithOptions(db, leanOpts)

	nodeLimits := model.NodeLimits{
		DefaultTimeout:   cfg.Engine.DefaultNodeTimeout,
		MaxTimeout:       cfg.Engine.MaxNodeTimeout,
		ConditionTimeout: cfg.Engine.ConditionTimeout,
		Parallel: model.ParallelLimits{
			MaxDepth:               cfg.Engine.Parallel.MaxDepth,
			MaxBranchesPerParallel: cfg.Engine.Parallel.MaxBranchesPerParallel,
		},
	}
	if nodeLimits.DefaultTimeout <= 0 {
		nodeLimits.DefaultTimeout = 30 * time.Second
	}
	if nodeLimits.MaxTimeout <= 0 {
		nodeLimits.MaxTimeout = 30 * time.Second
	}
	if nodeLimits.ConditionTimeout <= 0 {
		nodeLimits.ConditionTimeout = 5 * time.Second
	}

	limits := model.DefaultLimits()
	if cfg.Engine.MaxPerNodeExecutions > 0 {
		limits.MaxPerNodeExecutions = cfg.Engine.MaxPerNodeExecutions
	}
	if cfg.Engine.MaxTotalExecutions > 0 {
		limits.MaxTotalExecutions = cfg.Engine.MaxTotalExecutions
	}
	if cfg.Engine.LeaseDuration > 0 {
		limits.LeaseDuration = cfg.Engine.LeaseDuration
	}
	if cfg.Engine.ClaimBatchSize > 0 {
		limits.ClaimBatchSize = cfg.Engine.ClaimBatchSize
	}
	if cfg.Engine.Parallel.MaxActiveBranchesPerInstance > 0 {
		limits.MaxActiveBranchesPerInstance = cfg.Engine.Parallel.MaxActiveBranchesPerInstance
	}
	if cfg.Engine.Parallel.MaxDepth > 0 {
		limits.MaxParallelDepth = cfg.Engine.Parallel.MaxDepth
	}

	maxRedirects := cfg.Engine.MaxRedirects
	if maxRedirects <= 0 {
		maxRedirects = 5
	}
	execLimits := executor.Limits{
		HTTPAllowlist:  cfg.Engine.HTTPAllowlist,
		ExecAllowlist:  cfg.Engine.ExecAllowlist,
		MaxOutputBytes: cfg.Engine.MaxOutputBytes,
		MaxRedirects:   maxRedirects,
	}

	nodeSvc := service.NewNodeDefinitionService(nodeDefs, nodeLimits, actor)
	wfSvc := service.NewWorkflowDefinitionService(wfDefs, nodeDefs, nodeLimits, actor)
	secretSvc := service.NewSecretService(secrets)
	// mat resolves definition references for the engine loader directly,
	// keeping the engine's runtime path free of the service layer.
	mat := kernel.NewMaterializer(nodeDefs, nodeLimits)

	// Optional broker clients. They exist only when their DSN is configured;
	// a configured but unreachable broker fails startup. The close defers are
	// registered before the dispatcher/consumer defers below so shutdown
	// stops consumers and dispatchers before the connections close. The
	// interface-typed vars stay nil when the broker is absent.
	var redisClient *transport.RedisClient
	var redisPub transport.RedisPublisher
	if dsn := cfg.Infra.Redis.DSN; dsn != "" {
		rc, err := transport.NewRedisClient(ctx, dsn)
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		redisClient = rc
		redisPub = rc
		defer func() { _ = redisClient.Close() }()
		logger.Info("redis connected")
	}
	var rabbitClient *transport.RabbitClient
	var rabbitPub transport.RabbitPublisher
	if dsn := cfg.Infra.RabbitMQ.DSN; dsn != "" {
		rc, err := transport.NewRabbitClient(ctx, dsn,
			cfg.Infra.RabbitMQ.InputQueue, cfg.Infra.RabbitMQ.OutputQueue, cfg.Infra.RabbitMQ.StatusQueue)
		if err != nil {
			return fmt.Errorf("rabbitmq: %w", err)
		}
		rabbitClient = rc
		rabbitPub = rc
		defer func() { _ = rabbitClient.Close() }()
		logger.Info("rabbitmq connected")
	}

	hookRunner := executor.NewHookRunner(nil)
	executors := executor.NewExecutors(execLimits, nil, executor.Dependencies{
		Output:       executor.NewBrokerOutputPublisher(redisPub, rabbitPub, cfg.Infra.RabbitMQ.OutputQueue),
		RedisPoller:  redisClient,
		RabbitPoller: rabbitClient,
	})
	loader := func(ctx context.Context, instanceID string) (*model.WorkflowContent, error) {
		inst, err := instances.GetByID(ctx, instanceID)
		if err != nil {
			return nil, err
		}
		def, err := wfDefs.GetByID(ctx, inst.WorkflowDefinitionID)
		if err != nil {
			return nil, err
		}
		wc, err := model.ParseWorkflowContent(def.Content, nodeLimits)
		if err != nil {
			return nil, err
		}
		return mat.Materialize(ctx, wc)
	}
	eng := engine.NewEngine(instances, repository.NewParallelRepository(db), executors, hookRunner, limits, loader, actor, leanOpts)
	instSvc := service.NewInstanceServiceWithCatalog(
		instances,
		repository.NewParallelRepository(db),
		wfDefs,
		secrets,
		wfSvc,
		&executor.InputExecutor{},
		hookRunner,
		actor,
		nodeLimits,
		eng,
		leanOpts,
		catalog,
	)
	statsSvc := service.NewStatisticsService(instances)
	hostname, _ := os.Hostname()

	// Broker input consumers deliver payloads to waiting input nodes whose
	// channel matches the transport. They stop on ctx cancellation; the
	// consumer wait defer is registered before the dispatcher defers so
	// shutdown waits for in-flight deliveries before closing the brokers.
	var consumers sync.WaitGroup
	if redisClient != nil {
		redisInput := inputtransport.NewRedisInput(redisClient, instSvc)
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			if err := redisInput.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Warnf("redis input consumer stopped: %v", err)
			}
		}()
		logger.Info("redis input consumer started")
	}
	if rabbitClient != nil {
		rabbitInput := inputtransport.NewRabbitInput(rabbitClient, instSvc, "input-"+hostname)
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			if err := rabbitInput.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Warnf("rabbitmq input consumer stopped: %v", err)
			}
		}()
		logger.Info("rabbitmq input consumer started")
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		done := make(chan struct{})
		go func() {
			consumers.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-stopCtx.Done():
			logger.Warn("broker input consumers did not stop in time")
		}
	}()

	dispatcher, err := engine.NewDispatcher(ctx, eng, instances, repository.NewParallelRepository(db), "dispatcher-"+hostname, engine.DispatcherOptions{
		Lease:     limits.LeaseDuration,
		BatchSize: limits.ClaimBatchSize,
		PoolSize:  cfg.Worker.Pool.Size,
	})
	if err != nil {
		return fmt.Errorf("dispatcher: %w", err)
	}
	dispatcher.Run()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := dispatcher.Shutdown(shutdownCtx); err != nil {
			logger.Warnf("dispatcher shutdown: %v", err)
		}
	}()

	// Status-update dispatcher: drains the transactional outbox, delivering
	// notifications in strict per-instance and per-transport order through
	// the configured transport publishers (http always; redis and rabbitmq
	// when their clients exist).
	statusUpdates := repository.NewStatusUpdateRepository(db)
	statusLoader := func(ctx context.Context, definitionID string) (*model.StatusUpdateConfig, error) {
		def, err := wfDefs.GetByID(ctx, definitionID)
		if err != nil {
			return nil, err
		}
		return model.ParseStatusUpdate(def.Content)
	}
	statusPublishers := map[string]statusupdate.Publisher{
		model.StatusUpdateTransportHTTP: statusupdate.NewHTTPPublisher(executor.NewHTTPExecutor(execLimits), 0),
	}
	if redisPub != nil {
		statusPublishers[model.StatusUpdateTransportRedis] = statusupdate.NewRedisPublisher(redisPub)
	}
	if rabbitPub != nil {
		statusPublishers[model.StatusUpdateTransportRabbitMQ] = statusupdate.NewRabbitPublisher(rabbitPub, cfg.Infra.RabbitMQ.StatusQueue)
	}
	statusDispatcher, err := statusupdate.NewDispatcher(ctx, statusUpdates, statusLoader,
		statusPublishers, "status-"+hostname, statusupdate.DispatcherOptions{})
	if err != nil {
		return fmt.Errorf("status dispatcher: %w", err)
	}
	statusDispatcher.Run()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := statusDispatcher.Shutdown(shutdownCtx); err != nil {
			logger.Warnf("status dispatcher shutdown: %v", err)
		}
	}()

	// Cron scheduler: creates an instance of the target definition on
	// every tick of each enabled schedule. All enabled replicas fire and
	// the fire claim keeps each tick to one instance (see the scheduler
	// package); onChange refreshes the local entries after every
	// mutation, and other replicas catch up on the refresh interval.
	schedRepo := repository.NewScheduleRepository(db)
	var onScheduleChange func()
	if cfg.Scheduler.Enabled {
		cronScheduler := scheduler.New(ctx, schedRepo, instSvc, "scheduler-"+hostname, scheduler.SchedulerOptions{
			RefreshInterval: cfg.Scheduler.RefreshInterval,
		})
		onScheduleChange = cronScheduler.Refresh
		if err := cronScheduler.Run(); err != nil {
			return fmt.Errorf("scheduler: %w", err)
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			if err := cronScheduler.Shutdown(shutdownCtx); err != nil {
				logger.Warnf("scheduler shutdown: %v", err)
			}
		}()
	}
	schedSvc := service.NewScheduleService(schedRepo, wfDefs, actor, onScheduleChange)

	router := handler.NewRouter(handler.Deps{
		Health:              handler.NewHealth(sqlDB),
		NodeDefinitions:     nodeSvc,
		WorkflowDefinitions: wfSvc,
		Secrets:             secretSvc,
		Instances:           instSvc,
		Schedules:           schedSvc,
		Statistics:          statsSvc,
		Auth:                authSvc,
		SwaggerEnabled:      cfg.Infra.HTTP.SwaggerEnabled,
		AuthSettings:        cfg.Auth,
		OIDC:                oidcVerifier,
		Catalog:             catalog,
		SystemUserID:        actor,
	})

	srv := &http.Server{
		Addr:    cfg.Infra.HTTP.Host,
		Handler: router,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Infof("http server listening on %s", cfg.Infra.HTTP.Host)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		logger.Info("http server stopped")
		return nil
	case err := <-errCh:
		return fmt.Errorf("http serve: %w", err)
	}
}
