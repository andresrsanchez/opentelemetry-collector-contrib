package lavinmqreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/lavinmqreceiver/internal/metadata"
	"github.com/r3labs/sse"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/extension/experimental/storage"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestLogsReceiver_Collect(t *testing.T) {
	//default config?
	c := sse.NewClient("http://localhost:15672/api/livelog")
	c.Headers = map[string]string{
		"Authorization": "Basic Z3Vlc3Q6Z3Vlc3Q=",
	}
	obsr, _ := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             component.NewID(metadata.Type),
		ReceiverCreateSettings: receivertest.NewNopCreateSettings(),
	})
	r := &logsReceiver{
		config:        &Config{BufferSize: 1},
		client:        c,
		nextConsumer:  new(consumertest.LogsSink),
		clientStorage: storage.NewNopClient(),
		obsrecv:       obsr,
	}
	r.startCollecting(context.TODO())
	time.Sleep(3 * time.Second)
}
