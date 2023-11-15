package lavinmqreceiver

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/adapter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/lavinmqreceiver/internal/metadata"
	"github.com/r3labs/sse"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension/experimental/storage"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type Config struct {
	BufferSize int               `mapstructure:"buffer_size"`
	Url        string            `mapstructure:"url"`
	Headers    map[string]string `mapstructure:"headers"`
	StorageID  *component.ID     `mapstructure:"storage"`
}

func createDefaultConfig() component.Config {
	return &Config{
		BufferSize: 500,
	}
}

type logsReceiver struct {
	config        *Config
	settings      receiver.CreateSettings
	obsrecv       *receiverhelper.ObsReport
	nextConsumer  consumer.Logs
	id            component.ID
	cancel        context.CancelFunc
	client        *sse.Client
	curr_buffer   int
	clientStorage storage.Client
}

func createLogsReceiverFunc() receiver.CreateLogsFunc {
	return func(
		ctx context.Context,
		settings receiver.CreateSettings,
		config component.Config,
		consumer consumer.Logs,
	) (receiver.Logs, error) {
		c := config.(*Config)
		return newLogsReceiver(c, settings, consumer)
	}
}

func newLogsReceiver(
	config *Config,
	settings receiver.CreateSettings,
	nextConsumer consumer.Logs) (*logsReceiver, error) {

	obsr, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, err
	}
	c := sse.NewClient(config.Url)
	if config.Headers != nil {
		c.Headers = config.Headers
	}
	clientStorage := storage.NewNopClient() //todo configurable
	receiver := &logsReceiver{
		config:        config,
		settings:      settings,
		nextConsumer:  nextConsumer,
		id:            settings.ID,
		obsrecv:       obsr,
		client:        c,
		clientStorage: clientStorage,
	}

	return receiver, nil
}

func (receiver *logsReceiver) startCollecting(ctx context.Context) error {
	logs := plog.NewLogs()
	events := make(chan *sse.Event)
	err := receiver.client.SubscribeChanRawWithContext(ctx, events)
	if err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case msg, ok := <-events:
				if !ok {
					receiver.settings.Logger.Info("Channel closed. Exiting.")
					return
				}
				id := string(msg.ID)
				val, err := receiver.clientStorage.Get(ctx, id) //todo retry
				if val != nil {
					continue
				}
				if err != nil {
					receiver.settings.Logger.Error("failed to receive key from storage: %w", zap.Error(err))
				}
				receiver.curr_buffer++
				observedAt := pcommon.NewTimestampFromTime(time.Now())
				scopeLogs := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
				logRecord := scopeLogs.AppendEmpty()
				logRecord.Body().SetStr(string(msg.Data))
				logRecord.SetObservedTimestamp(observedAt)

				if t, err := strconv.ParseInt(id, 10, 64); err == nil {
					timestamp := pcommon.NewTimestampFromTime(time.Unix(0, t*int64(time.Millisecond)))
					logRecord.SetTimestamp(timestamp)
				}
				if receiver.curr_buffer == receiver.config.BufferSize {
					logs = receiver.sendLogs(ctx, &logs, id)
					ticker.Reset(time.Second)
				}

			case <-ticker.C:
				logs = receiver.sendLogs(ctx, &logs, "")
			case <-ctx.Done():
				receiver.settings.Logger.Info("Done. Exiting.")
				return
			}
		}
	}()
	return nil
}

func (receiver *logsReceiver) sendLogs(ctx context.Context, logs *plog.Logs, id string) plog.Logs {
	ctx = receiver.obsrecv.StartLogsOp(ctx)
	err := receiver.nextConsumer.ConsumeLogs(ctx, *logs)
	receiver.obsrecv.EndLogsOp(ctx, metadata.Type, receiver.config.BufferSize, err)
	if err != nil {
		receiver.settings.Logger.Error("failed to send logs: %w", zap.Error(err))
	} else if id != "" {
		_ = receiver.clientStorage.Set(ctx, id, nil) //todo retry
	}
	receiver.curr_buffer = 0
	return plog.NewLogs() //review
}

func (receiver *logsReceiver) Start(ctx context.Context, host component.Host) error {
	rctx, cancel := context.WithCancel(ctx)
	receiver.cancel = cancel
	var err error
	receiver.clientStorage, err = adapter.GetStorageClient(ctx, host, receiver.config.StorageID, receiver.settings.ID)
	if err != nil {
		return fmt.Errorf("error connecting to storage: %w", err)
	}
	return receiver.startCollecting(rctx)
}

func (receiver *logsReceiver) Shutdown(context.Context) error {
	if receiver.cancel == nil {
		return nil
	}
	receiver.cancel()
	return nil
}
