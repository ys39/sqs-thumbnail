package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	appconfig "worker/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/disintegration/imaging"
	"github.com/google/uuid"
)

// 設定関連の構造体
type Config struct {
	QueueURL          string
	MaxMessages       int32
	WaitTimeSeconds   int32
	VisibilityTimeout int32
	NumWorkers        int
	RetryDelay        time.Duration
	S3Bucket          string
}

// ワーカー構造体
type Worker struct {
	config    *Config
	sqsClient *sqs.Client
	s3Client  *s3.Client
	logger    *log.Logger
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

// デフォルト設定
func defaultConfig() *Config {
	return &Config{
		QueueURL:          appconfig.AWS_SQS_QUEUE_URL,
		MaxMessages:       10,
		WaitTimeSeconds:   10,
		VisibilityTimeout: 30,
		NumWorkers:        3,
		RetryDelay:        5 * time.Second,
		S3Bucket:          appconfig.AWS_S3_BUCKET,
	}
}

// 新しいワーカーインスタンスの作成
func NewWorker(cfg *Config) (*Worker, error) {
	if cfg == nil {
		cfg = defaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(appconfig.AWS_REGION),
		config.WithSharedConfigProfile(appconfig.AWS_PROFILE),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// SQS クライアント初期化
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(appconfig.AWS_ENDPOINT)
	})

	// S3 クライアント初期化
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(appconfig.AWS_ENDPOINT)
	})

	return &Worker{
		config:    cfg,
		sqsClient: sqsClient,
		s3Client:  s3Client,
		logger:    log.New(os.Stdout, "[WORKER] ", log.LstdFlags),
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// メッセージ処理
func (w *Worker) processMessage(msg *types.Message) error {
	s3Key := aws.ToString(msg.Body)
	w.logger.Printf("Processing message: S3Key=%s", s3Key)

	// サムネイル生成処理
	if err := w.generateThumbnail(s3Key); err != nil {
		return fmt.Errorf("failed to generate thumbnail: %w", err)
	}

	// 処理済みメッセージの削除
	if err := w.deleteMessage(msg); err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	return nil
}

// サムネイル生成処理
func (w *Worker) generateThumbnail(s3Key string) error {
	// 1. S3からファイルをダウンロード
	input := &s3.GetObjectInput{
		Bucket: aws.String(w.config.S3Bucket),
		Key:    aws.String(s3Key),
	}

	result, err := w.s3Client.GetObject(w.ctx, input)
	if err != nil {
		return fmt.Errorf("S3からのダウンロードに失敗: %w", err)
	}
	defer result.Body.Close()

	// ローカルファイルを作成
	localFile, err := os.Create("/tmp/" + s3Key)
	if err != nil {
		return fmt.Errorf("ローカルファイルの作成に失敗: %w", err)
	}
	defer localFile.Close()

	// S3のオブジェクトをローカルファイルにコピー
	if _, err = io.Copy(localFile, result.Body); err != nil {
		return fmt.Errorf("ファイルの書き込みに失敗: %w", err)
	}

	// 2. サムネイル生成
	// 画像ファイルを開く
	img, err := imaging.Open("/tmp/" + s3Key)
	if err != nil {
		return fmt.Errorf("画像ファイルのオープンに失敗: %w", err)
	}

	// 100x100にリサイズ
	thumbnail := imaging.Resize(img, 100, 100, imaging.Lanczos)

	// サムネイルを一時ファイルとして保存
	thumbnailPath := "/tmp/thumb_" + s3Key
	err = imaging.Save(thumbnail, thumbnailPath)
	if err != nil {
		return fmt.Errorf("サムネイルの保存に失敗: %w", err)
	}

	// サムネイルファイルを開く
	thumbnailFile, err := os.Open(thumbnailPath)
	if err != nil {
		return fmt.Errorf("サムネイルファイルのオープンに失敗: %w", err)
	}
	defer thumbnailFile.Close()

	// S3にアップロード
	objectKey := fmt.Sprintf("%s/%s.jpg", "thumbnail", uuid.New().String()[:10])
	_, err = w.s3Client.PutObject(w.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.config.S3Bucket),
		Key:    aws.String(objectKey),
		Body:   thumbnailFile,
	})
	if err != nil {
		return fmt.Errorf("S3へのアップロードに失敗: %w", err)
	}

	return nil
}

// メッセージの削除
func (w *Worker) deleteMessage(msg *types.Message) error {
	_, err := w.sqsClient.DeleteMessage(w.ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(w.config.QueueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	return err
}

// 単一ワーカーの処理ループ
func (w *Worker) workerLoop(workerID int) {
	defer w.wg.Done()
	w.logger.Printf("Worker %d started", workerID)

	for {
		select {
		case <-w.ctx.Done():
			w.logger.Printf("Worker %d shutting down", workerID)
			return
		default:
			if err := w.receiveAndProcessMessages(); err != nil {
				w.logger.Printf("Worker %d encountered error: %v", workerID, err)
				time.Sleep(w.config.RetryDelay)
			}
		}
	}
}

// メッセージの受信と処理
func (w *Worker) receiveAndProcessMessages() error {
	msgs, err := w.sqsClient.ReceiveMessage(w.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(w.config.QueueURL),
		MaxNumberOfMessages: w.config.MaxMessages,
		WaitTimeSeconds:     w.config.WaitTimeSeconds,
		VisibilityTimeout:   w.config.VisibilityTimeout,
	})
	if err != nil {
		return fmt.Errorf("failed to receive messages: %w", err)
	}

	for _, msg := range msgs.Messages {
		if err := w.processMessage(&msg); err != nil {
			w.logger.Printf("Error processing message: %v", err)
			// エラーログを記録して続行
			continue
		}
	}

	return nil
}

// ワーカーの起動
func (w *Worker) Start() {
	w.logger.Println("Starting workers...")

	// 指定された数のワーカーを起動
	for i := 0; i < w.config.NumWorkers; i++ {
		w.wg.Add(1)
		go w.workerLoop(i)
	}

	// シグナルハンドリング
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// シグナルを待機
	<-sigChan
	w.logger.Println("Shutdown signal received")

	// グレースフルシャットダウン
	w.Shutdown()
}

// ワーカーのシャットダウン
func (w *Worker) Shutdown() {
	w.logger.Println("Initiating graceful shutdown...")
	w.cancel()

	// 全ワーカーの終了を待機
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	// タイムアウト付きで待機
	select {
	case <-done:
		w.logger.Println("All workers shutdown successfully")
	case <-time.After(30 * time.Second):
		w.logger.Println("Shutdown timed out")
	}
}

func main() {
	worker, err := NewWorker(nil)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	worker.Start()
}
