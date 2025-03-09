package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	appconfig "api/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
)

// グローバル変数
var (
	sqsClient *sqs.Client
	s3Client  *s3.Client
	queueURL  string
	s3Bucket  string
)

// レスポンス用の構造体
type Response struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// エラーレスポンスを返す関数
func sendErrorResponse(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(Response{
		Success: false,
		Message: message,
	})
}

// 成功レスポンスを返す関数
func sendSuccessResponse(w http.ResponseWriter, message string, data any, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(Response{
		Success: true,
		Message: message,
		Data:    data,
	})
}

func main() {
	// コンテキストの作成
	ctx, cancel := context.WithCancel(context.Background())

	// AWS Config の読み込み
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(appconfig.AWS_REGION),
		config.WithSharedConfigProfile(appconfig.AWS_PROFILE),
	)
	if err != nil {
		cancel()
		log.Fatal(err)
	}

	// SQS クライアント初期化
	sqsClient = sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(appconfig.AWS_ENDPOINT)
	})

	// S3 クライアント初期化
	s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(appconfig.AWS_ENDPOINT)
	})

	// キューURLとバケット名の設定
	queueURL = appconfig.AWS_SQS_QUEUE_URL
	s3Bucket = appconfig.AWS_S3_BUCKET

	// エンドポイント定義
	http.HandleFunc("/upload", handleUpload)

	fmt.Println("Starting server at :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// 画像アップロードを受け付ける例
func handleUpload(w http.ResponseWriter, r *http.Request) {
	// リクエストファイルをS3にアップロード
	// ここでは簡易的にローカルのファイルをアップロードする
	filePath := "./test.jpg"
	file, err := os.Open(filePath)
	if err != nil {
		sendErrorResponse(w, "failed to open file", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// S3にアップロード
	// objectKeyは10文字のランダムな文字列 + .jpg
	objectKey := fmt.Sprintf("%s.jpg", uuid.New().String()[:10])
	_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(objectKey),
		Body:   file,
	})
	if err != nil {
		log.Println(err)
		sendErrorResponse(w, "failed to upload to S3", http.StatusInternalServerError)
		return
	}

	// SQS へ送信
	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(objectKey),
	}

	_, err = sqsClient.SendMessage(context.TODO(), input)
	if err != nil {
		sendErrorResponse(w, "failed to send message", http.StatusInternalServerError)
		return
	}

	// 成功時のレスポンス
	sendSuccessResponse(w, "Successfully uploaded file and sent message", nil, http.StatusCreated)
}
