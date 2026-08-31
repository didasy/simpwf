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
	// @description API token authentication via the X-Api-Token header. Enabled when auth.enabled is true.

	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/engine"
	"github.com/simpwf/workflow-engine/internal/workflow/executor"
	"github.com/simpwf/workflow-engine/internal/workflow/handler"
	"github.com/simpwf/workflow-engine/internal/workflow/inputtransport"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/internal/workflow/statusupdate"
	"github.com/simpwf/workflow-engine/internal/workflow/transport"
	"github.com/simpwf/workflow-engine/pkg/configuration"
	"github.com/simpwf/workflow-engine/pkg/database"
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

// run starts the HTTP server and the dispatcher, and blocks until ctx is
// cancelled, then shuts down gracefully. It returns nil on a clean shutdown.
func run(ctx context.Context, cfg *configuration.Config, logger *logrus.Logger) error {
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
	instances := repository.NewInstanceRepository(db)

	nodeLimits := model.NodeLimits{
		DefaultTimeout:   cfg.Engine.DefaultNodeTimeout,
		MaxTimeout:       cfg.Engine.MaxNodeTimeout,
		ConditionTimeout: cfg.Engine.ConditionTimeout,
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
		return wfSvc.Materialize(ctx, wc)
	}
	eng := engine.NewEngine(instances, executors, hookRunner, limits, loader, actor)
	instSvc := service.NewInstanceService(instances, wfDefs, wfSvc, &executor.InputExecutor{}, hookRunner, actor, nodeLimits, eng)
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

	dispatcher, err := engine.NewDispatcher(ctx, eng, instances, "dispatcher-"+hostname, engine.DispatcherOptions{
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

	router := handler.NewRouter(handler.Deps{
		Health:              handler.NewHealth(sqlDB),
		NodeDefinitions:     nodeSvc,
		WorkflowDefinitions: wfSvc,
		Instances:           instSvc,
		SwaggerEnabled:      cfg.Infra.HTTP.SwaggerEnabled,
		Auth:                cfg.Auth,
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
