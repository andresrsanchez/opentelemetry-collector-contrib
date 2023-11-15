package lavinmqreceiver

import (
	"context"
	"strconv"
	"time"

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
	Url        string            `mapstructure:"buffer_size"`
	Headers    map[string]string `mapstructure:"headers"`
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
	// c := sse.NewClient("http://localhost:15672/api/livelog")
	// c.Headers = map[string]string{
	// 	"Authorization": "Basic Z3Vlc3Q6Z3Vlc3Q=",
	// }
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

func (receiver *logsReceiver) startCollecting(ctx context.Context) {
	logs := plog.NewLogs()
	receiver.client.SubscribeWithContext(ctx, "", func(msg *sse.Event) {
		val, err := receiver.clientStorage.Get(ctx, string(msg.ID)) //todo retry
		if val != nil {
			return
		}
		if err != nil {
			receiver.settings.Logger.Error("failed to receive key from storage: %w", zap.Error(err))
		}
		receiver.curr_buffer--
		observedAt := pcommon.NewTimestampFromTime(time.Now())
		scopeLogs := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
		logRecord := scopeLogs.AppendEmpty()
		logRecord.Body().SetStr(string(msg.Data))
		logRecord.SetObservedTimestamp(observedAt)

		if t, err := strconv.ParseInt(string(msg.ID), 10, 64); err == nil {
			timestamp := pcommon.NewTimestampFromTime(time.Unix(0, t*int64(time.Millisecond)))
			logRecord.SetTimestamp(timestamp)
		}
		if receiver.curr_buffer == 0 {
			ctx := receiver.obsrecv.StartLogsOp(context.Background())
			err := receiver.nextConsumer.ConsumeLogs(context.Background(), logs)
			receiver.obsrecv.EndLogsOp(ctx, metadata.Type, receiver.config.BufferSize, err)
			if err != nil {
				receiver.settings.Logger.Error("failed to send logs: %w", zap.Error(err))
			} else {
				_ = receiver.clientStorage.Set(ctx, string(msg.ID), nil) //todo retry
			}
			receiver.curr_buffer = receiver.config.BufferSize
			logs = plog.NewLogs()
		}
	})
}

func (receiver *logsReceiver) Start(ctx context.Context, host component.Host) error {
	rctx, cancel := context.WithCancel(ctx)
	receiver.cancel = cancel
	go receiver.startCollecting(rctx)

	return nil
}

func (receiver *logsReceiver) Shutdown(context.Context) error {
	if receiver.cancel == nil {
		return nil
	}
	receiver.cancel()
	return nil
}
