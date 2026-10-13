package logger

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"sync"
)

const notConfigured = "[ENCRYPTOR_NOT_CONFIGURED]"

type Encryptor interface {
	Encrypt([]byte) (string, error)
	KeyID() string
}

type EncryptedLogger struct {
	encryptor Encryptor
	mutex     sync.RWMutex
}

var encryptedLogger *EncryptedLogger

func init() {
	var enc Encryptor

	if key := os.Getenv("ENCRYPTOR_KEY"); key != "" {
		keyID := os.Getenv("ENCRYPTOR_KEY_ID")
		enc = NewRealEncryptor(keyID, key)
	} else {
		enc = &dummyEncryptor{}
	}

	SetEncryptedLogger(enc)
}

func SetEncryptedLogger(enc Encryptor) {
	encryptedLogger = &EncryptedLogger{encryptor: enc}
}

func Encrypted(data string) string {
	if encryptedLogger == nil {
		return "[ENCRYPTED_LOGGER_NOT_SET]"
	}
	return encryptedLogger.Encrypt(data)
}

// EncryptWithKeyID returns both the key ID and the encrypted value
func EncryptWithKeyID(data string) (keyID, encrypted string) {
	if encryptedLogger == nil || encryptedLogger.encryptor == nil {
		return notConfigured, notConfigured
	}
	return encryptedLogger.encryptor.KeyID(), encryptedLogger.Encrypt(data)
}

func (e *EncryptedLogger) Encrypt(data string) string {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if e.encryptor == nil {
		return notConfigured
	}

	encrypted, err := e.encryptor.Encrypt([]byte(data))
	if err != nil {
		return "[ENCRYPTION_FAILED]"
	}

	return encrypted
}

// Real encryptor using AES-GCM
type realEncryptor struct {
	keyID string
	key   []byte
}

func NewRealEncryptor(keyID, keyStr string) Encryptor {
	key := []byte(keyStr)
	if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		panic(errors.New("ENCRYPTOR_KEY must be 16, 24, or 32 bytes long"))
	}
	return &realEncryptor{
		keyID: keyID,
		key:   key,
	}
}

func KeyID() string {
	if encryptedLogger == nil || encryptedLogger.encryptor == nil {
		return notConfigured
	}
	return encryptedLogger.encryptor.KeyID()
}

func (r *realEncryptor) Encrypt(data []byte) (string, error) {
	block, err := aes.NewCipher(r.key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func (r *realEncryptor) KeyID() string {
	return r.keyID
}

// Dummy encryptor
type dummyEncryptor struct{}

func (d *dummyEncryptor) Encrypt(_ []byte) (string, error) {
	return "ENC", nil
}

func (d *dummyEncryptor) KeyID() string {
	return "dummy-key"
}
