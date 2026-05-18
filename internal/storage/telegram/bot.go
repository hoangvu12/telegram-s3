package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"telegram-s3/internal/storage"
)

type BotStorage struct {
	token  string
	chatID string
	client *http.Client
	logger *slog.Logger
}

func NewBotStorage(token, chatID string, logger *slog.Logger) *BotStorage {
	return &BotStorage{
		token:  token,
		chatID: chatID,
		client: &http.Client{Timeout: 0},
		logger: logger,
	}
}

func (b *BotStorage) Upload(ctx context.Context, name, contentType string, body io.Reader) (storage.UploadedObject, error) {
	var payload bytes.Buffer
	writer := multipart.NewWriter(&payload)

	if err := writer.WriteField("chat_id", b.chatID); err != nil {
		return storage.UploadedObject{}, err
	}
	part, err := writer.CreateFormFile("document", safeFilename(name))
	if err != nil {
		return storage.UploadedObject{}, err
	}
	if _, err := io.Copy(part, body); err != nil {
		return storage.UploadedObject{}, err
	}
	if err := writer.Close(); err != nil {
		return storage.UploadedObject{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.apiURL("sendDocument"), &payload)
	if err != nil {
		return storage.UploadedObject{}, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := b.client.Do(req)
	if err != nil {
		return storage.UploadedObject{}, err
	}
	defer resp.Body.Close()

	var result sendDocumentResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return storage.UploadedObject{}, err
	}
	if resp.StatusCode >= 300 || !result.OK {
		return storage.UploadedObject{}, fmt.Errorf("telegram sendDocument failed: %s", result.Description)
	}
	if len(result.Result.Document.FileID) == 0 {
		return storage.UploadedObject{}, fmt.Errorf("telegram response did not include document file_id")
	}

	return storage.UploadedObject{FileID: result.Result.Document.FileID, MessageID: result.Result.MessageID}, nil
}

func (b *BotStorage) Download(ctx context.Context, fileID string) (io.ReadCloser, error) {
	reqBody := strings.NewReader("file_id=" + fileID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.apiURL("getFile"), reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result getFileResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 || !result.OK {
		return nil, fmt.Errorf("telegram getFile failed: %s", result.Description)
	}

	downloadReq, err := http.NewRequestWithContext(ctx, http.MethodGet, b.fileURL(result.Result.FilePath), nil)
	if err != nil {
		return nil, err
	}
	downloadResp, err := b.client.Do(downloadReq)
	if err != nil {
		return nil, err
	}
	if downloadResp.StatusCode >= 300 {
		downloadResp.Body.Close()
		return nil, fmt.Errorf("telegram file download failed: %s", downloadResp.Status)
	}
	return downloadResp.Body, nil
}

func (b *BotStorage) apiURL(method string) string {
	return fmt.Sprintf("https://api.telegram.org/bot%s/%s", b.token, method)
}

func (b *BotStorage) fileURL(path string) string {
	return fmt.Sprintf("https://api.telegram.org/file/bot%s/%s", b.token, path)
}

func safeFilename(name string) string {
	name = strings.TrimSpace(strings.ReplaceAll(name, "\\", "/"))
	if name == "" || strings.HasSuffix(name, "/") {
		return fmt.Sprintf("object-%d.bin", time.Now().UnixNano())
	}
	parts := strings.Split(name, "/")
	return parts[len(parts)-1]
}

type sendDocumentResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
	Result      struct {
		MessageID int64 `json:"message_id"`
		Document  struct {
			FileID string `json:"file_id"`
		} `json:"document"`
	} `json:"result"`
}

type getFileResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
	Result      struct {
		FilePath string `json:"file_path"`
	} `json:"result"`
}
