package relay

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/google/uuid"

	"github.com/Amisha0612/evb/pkg/logger"
)

type blob struct {
	client        *container.Client
	id            string
	AccountName   string `yaml:"storageName"`
	ContainerName string `yaml:"container"`
	ServiceURL    string `yaml:"serviceUrl"`
	PathPrefix    string `yaml:"pathPrefix"`
	SasToken      string `yaml:"sasToken"`
}

type blobController interface {
	GetID() string
	Transport(context.Context, any) (any, error)
}

func newBlob(obj *blob) blobController {
	client, err := container.NewClientWithNoCredential(fmt.Sprintf("%s/%s%s", obj.ServiceURL, obj.ContainerName, obj.SasToken), nil)
	if err != nil {
		logger.Errorf("error initializing blob client for %s: %v", obj.AccountName, err)
		return nil
	}

	return &blob{
		client:        client,
		id:            uuid.New().String(),
		AccountName:   obj.AccountName,
		ContainerName: obj.ContainerName,
		ServiceURL:    obj.ServiceURL,
		PathPrefix:    obj.PathPrefix,
		SasToken:      obj.SasToken,
	}
}

func (b *blob) Transport(ctx context.Context, payload any) (any, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	// for poc the blob name will be union of the path prefix and uuid
	blobName := fmt.Sprintf("%s%s.json", b.PathPrefix, uuid.New().String())
	blobClient := b.client.NewBlockBlobClient(blobName)

	_, err = blobClient.UploadBuffer(ctx, data, &blockblob.UploadBufferOptions{})
	if err != nil {
		logger.Errorf("failed to upload blob %s: %v", blobName, err)
		return nil, err
	}

	return nil, nil
}

func (b *blob) GetID() string {
	return b.id
}
