package ingestpdf

import (
	"context"
	"emperror.dev/errors"
	"github.com/je4/filesystem/v3/pkg/writefs"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

func NewIngester(dbClient mediaserverproto.DatabaseClient, vfs fs.FS, concurrentWorkers int, ingestTimeout, ingestWait time.Duration, gsPath string, gsParams []string, tempDir string, logger zLogger.ZLogger) (*IngesterPdf, error) {
	if concurrentWorkers < 1 {
		return nil, errors.New("concurrentWorkers must be at least 1")
	}
	if ingestTimeout < 1 {
		return nil, errors.New("ingestTimeout must not be 0")
	}
	i := &IngesterPdf{
		dbClient:      dbClient,
		gsPath:        gsPath,
		gsParams:      gsParams,
		tempDir:       tempDir,
		end:           make(chan bool),
		jobChan:       make(chan *JobStruct),
		ingestTimeout: ingestTimeout,
		ingestWait:    ingestWait,
		logger:        logger,
		vfs:           vfs,
	}
	i.jobChan, i.worker = NewWorkerPool(concurrentWorkers, ingestTimeout, i.doIngest, logger)

	return i, nil
}

type IngesterPdf struct {
	dbClient      mediaserverproto.DatabaseClient
	end           chan bool
	worker        io.Closer
	jobChan       chan *JobStruct
	ingestTimeout time.Duration
	ingestWait    time.Duration
	logger        zLogger.ZLogger
	vfs           fs.FS
	gsPath        string
	tempDir       string
	gsParams      []string
}
type WriterNopcloser struct {
	io.Writer
}

func (WriterNopcloser) Close() error { return nil }

func (i *IngesterPdf) doIngest(job *JobStruct) error {
	i.logger.Debug().Msgf("ingestpdf %s/%s", job.collection, job.signature)

	item, err := i.dbClient.GetItem(context.Background(), &mediaserverproto.ItemIdentifier{
		Collection: job.collection,
		Signature:  job.signature,
	})
	if err != nil {
		return errors.Wrapf(err, "cannot get item %s/%s", job.collection, job.signature)
	}

	var fullpath string
	if !strings.Contains(job.Path, "://") {
		fullpath = strings.Join([]string{job.Storage.Filebase, job.Path}, "/")
	} else {
		fullpath = job.Path
	}
	itemName := createCacheName(job.collection, job.signature+"$$cover", "cover.png")
	itemPath := job.Storage.Filebase + "/" + filepath.ToSlash(filepath.Join(job.Storage.Subitemdir, itemName))

	if err := func() error {
		sourceReader, err := i.vfs.Open(fullpath)
		if err != nil {
			return errors.Wrapf(err, "cannot open %s", fullpath)
		}
		defer sourceReader.Close()

		command := i.gsPath
		args := append(i.gsParams, "-sOutputFile=-", "-")
		targetWriter, err := writefs.Create(i.vfs, itemPath)
		if err != nil {
			return errors.Wrapf(err, "cannot create %s", itemPath)
		}
		var removeTarget = false
		defer func() {
			if err := targetWriter.Close(); err != nil {
				i.logger.Error().Err(err).Msgf("cannot close %s", itemPath)
			}
			if removeTarget {
				if err := writefs.Remove(i.vfs, itemPath); err != nil {
					i.logger.Error().Err(err).Msgf("cannot remove %s", itemPath)
				}
			}
		}()
		i.logger.Debug().Msgf("run %s %s", command, strings.Join(args, " "))
		cmd := exec.Command(command, args...)
		cmd.Stdin = sourceReader
		cmd.Stdout = targetWriter
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			removeTarget = true
			return errors.Wrapf(err, "cannot run %s %v", command, args)
		}
		return nil
	}(); err != nil {
		return errors.WithStack(err)
	}

	var public = item.GetPublic() || slices.Contains(item.GetPublicActions(), "cover")
	var ingestType = mediaserverproto.IngestType_KEEP

	resp, err := i.dbClient.CreateItem(context.Background(), &mediaserverproto.NewItem{
		Identifier: &mediaserverproto.ItemIdentifier{
			Collection: job.collection,
			Signature:  job.signature + "$$cover",
		},
		Parent: &mediaserverproto.ItemIdentifier{
			Collection: job.collection,
			Signature:  job.signature,
		},
		Urn:        itemPath,
		IngestType: &ingestType,
		Public:     &public,
	})
	if err != nil {
		return errors.Wrapf(err, "cannot create item %s/%s", job.collection, job.signature+"$$cover")
	}
	i.logger.Info().Msgf("created item %s/%s: %s", job.collection, job.signature+"$$cover", resp.GetMessage())
	return nil
}

func (i *IngesterPdf) Start() error {
	go func() {
		for {
			for {
				item, err := i.dbClient.GetDerivateIngestItem(context.Background(), &mediaserverproto.DerivatIngestRequest{
					Type:    "text",
					Subtype: "pdf",
					Suffix:  []string{"$$cover"},
				})
				if err != nil {
					if s, ok := status.FromError(err); ok {
						if s.Code() == codes.NotFound {
							i.logger.Info().Msg("no ingest item available")
						} else {
							i.logger.Error().Err(err).Msg("cannot get ingest item")
						}
					} else {
						i.logger.Error().Err(err).Msg("cannot get ingest item")
					}
					break // on all errors we break
				}
				cache, err := i.dbClient.GetCache(context.Background(), &mediaserverproto.CacheRequest{
					Identifier: item.Item.GetIdentifier(),
					Action:     "item",
					Params:     "",
				})
				if err != nil {
					i.logger.Error().Err(err).Msgf("cannot get cache %s/%s/item", item.Item.GetIdentifier().GetCollection(), item.Item.GetIdentifier().GetSignature())
					break
				}
				job := &JobStruct{
					collection: item.Item.GetIdentifier().GetCollection(),
					signature:  item.Item.GetIdentifier().GetSignature(),
					Width:      cache.GetMetadata().GetWidth(),
					Height:     cache.GetMetadata().GetHeight(),
					Duration:   cache.GetMetadata().GetDuration(),
					Size:       cache.GetMetadata().GetSize(),
					MimeType:   cache.GetMetadata().GetMimeType(),
					Path:       cache.GetMetadata().GetPath(),
					Missing:    item.GetMissing(),
					Storage: &storageStruct{
						Name:       cache.GetMetadata().GetStorage().GetName(),
						Filebase:   cache.GetMetadata().GetStorage().GetFilebase(),
						Datadir:    cache.GetMetadata().GetStorage().GetDatadir(),
						Subitemdir: cache.GetMetadata().GetStorage().GetSubitemdir(),
						Tempdir:    cache.GetMetadata().GetStorage().GetTempdir(),
					},
				}
				i.jobChan <- job
				i.logger.Debug().Msgf("ingest video item %s/%s", job.collection, job.signature)
				// check for end without blocking
				select {
				case <-i.end:
					close(i.end)
					return
				default:
				}
			}
			select {
			case <-i.end:
				close(i.end)
				return
			case <-time.After(i.ingestWait):
			}
		}
	}()
	return nil
}

func (i *IngesterPdf) Close() error {
	i.end <- true
	return i.worker.Close()
}
