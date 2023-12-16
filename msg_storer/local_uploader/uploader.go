package uploader

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/weedbox/common-modules/nats_connector"
)

const (
	DefaultDomain       = "onglai-msg"
	DefaultSubject      = "%s.archive.bucket.job.%s"
	DefaultDatastore    = "/datastore"
	DefaultArchivestore = "/archivestore"
)

type Uploader struct {
	params       Params
	logger       *zap.Logger
	scope        string
	domain       string
	datastore    string
	archivestore string
	hostname     string
}

type Params struct {
	fx.In
	NATSConnector *nats_connector.NATSConnector
	Lifecycle     fx.Lifecycle
	Logger        *zap.Logger
}

func Module(scope string) fx.Option {

	var u *Uploader

	return fx.Options(
		fx.Provide(func(p Params) *Uploader {

			u = &Uploader{
				params: p,
				logger: p.Logger.Named(scope),
				scope:  scope,
			}
			u.initDefaultConfigs()
			return u
		}),
		fx.Populate(&u),
		fx.Invoke(func(p Params) {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: u.onStart,
					OnStop:  u.onStop,
				},
			)
		}),
	)

}

func (u *Uploader) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", u.scope, key)
}

func (u *Uploader) initDefaultConfigs() {
	viper.SetDefault(u.getConfigPath("archive_domain"), DefaultDomain)
	viper.SetDefault(u.getConfigPath("datastore"), DefaultDatastore)
	viper.SetDefault(u.getConfigPath("archivestore"), DefaultArchivestore)
}

func (u *Uploader) onStart(ctx context.Context) error {

	u.logger.Info("Starting Uploader")

	u.domain = viper.GetString(u.getConfigPath("archive_domain"))
	u.datastore = viper.GetString(u.getConfigPath("datastore"))
	u.archivestore = viper.GetString(u.getConfigPath("archivestore"))

	//get hostname
	hostname, err := os.Hostname()
	if err != nil {
		u.logger.Fatal(err.Error())
	}
	u.hostname = hostname

	err = u.startSubscriber()
	if err != nil {
		return err
	}

	return nil
}

func (u *Uploader) onStop(ctx context.Context) error {
	u.logger.Info("Stopped Uploader")

	return nil
}

func (u *Uploader) startSubscriber() error {
	// nats stream pub a msg to cloud-uploader
	js := u.params.NATSConnector.GetJetStreamContext()
	subject := fmt.Sprintf(DefaultSubject, u.domain, u.hostname)
	go func() {
		//u.logger.Info(subject)
		_, err := js.Subscribe(subject,
			u.msgHandler,
			nats.ManualAck(),
		)
		if err != nil {
			u.logger.Fatal(err.Error())
		}
	}()
	return nil
}

func (u *Uploader) updateIndex(filename string, archiveName string, seq string) error {

	// prepare data
	data := fmt.Sprintf("%s:%s\n", seq, archiveName)

	// opend index file
	dstDir := path.Dir(filename)
	indexFilename := path.Join(dstDir, "archive.index")
	indexFile, err := os.OpenFile(indexFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	// write index file
	_, err = indexFile.WriteString(data)
	if err != nil {
		return err
	}
	return nil
}

func (u *Uploader) msgHandler(m *nats.Msg) {
	mdata := strings.SplitN(string(m.Data), ":", 2)
	filename := mdata[1]

	archiveName := strings.ReplaceAll(filename, path.Join(u.datastore), path.Join(u.archivestore))

	err := os.MkdirAll(path.Dir(archiveName), 0750)
	if err != nil {
		m.Nak()
		u.logger.Error(err.Error())
		return
	}

	u.logger.Debug("Archive file",
		zap.String("fileName", filename),
		zap.String("archiveName", archiveName),
	)

	if err := os.Rename(filename, archiveName); err != nil {
		m.Nak()
		u.logger.Error(err.Error())
		return
	}

	//update indexFile
	err = u.updateIndex(filename, archiveName, mdata[0])
	if err != nil {
		m.Nak()
		u.logger.Error(err.Error())
		return
	}

	m.Ack()
}
