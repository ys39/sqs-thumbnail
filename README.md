---
title: 'SQS アーキテクチャ1'
date: '2025-02-27'
tags: ['SQS', 'localstack', 'Golang', 'Architecture']
isOpen: true
---
### はじめに
* Web開発において、同期処理だけでは対応できない場合や、非同期処理の方が適している場合がある。例えば、サムネイル生成、メール送信、ログの保存など。利用ユーザーはリアルタイム性を求めていないもしくは、その存在を気にしていないケースがあり、そのような場合において非同期処理は有効である。
- 非同期処理を実装するにあたり、AWSのSQSやSNS等のメッセージキューサービスを利用することが多い。本記事では、SQSを利用した非同期処理のアーキテクチャについて簡単に実装し、SQSの基本や使われ方を理解することを目的とする。

### そもそもSQSとは
* SQS（Amazon Simple Queue Service） は、AWSが提供するメッセージキューサービス
* アプリケーション同士がやり取りするメッセージを一時的にキューに蓄え、「送信（プロデューサ）」、「受信（コンシューマ）」の非同期化を実現するためのサービスとして利用される。
* コンシューマに対してメッセージを取りに行くプル型を採用している

### SQSの特徴
* 可視性タイムアウト .. ワーカーがメッセージを取得すると、一定期間ほかのワーカーからはそのメッセージが見えなくなる仕組み。処理が終わらずタイムアウトになると、メッセージは再び可視化され、他のワーカーが再取得できるようになる。
* Dead Letter Queue (DLQ) .. 何度も処理に失敗するメッセージを別のキュー（DLQ）に移動させることで、本来のキューを「詰まり」から守る機能がある。
* 種類 (Standard / FIFO)
    * Standard Queue: メッセージの順序は保証されないが、スループットが高い。
    * FIFO Queue: メッセージの順序を厳密に保証し、重複を防止する仕組みもあるが、スループットはやや低め。

### 作成するWebアプリケーションの概要

* **サムネイル生成アプリケーション**を作成する
* 登場する要素は以下の通り

    1. Webアプリ（APIサーバ）
        - ユーザーから画像アップロードやタスク依頼を受け付ける。
        - 受け付けたリクエストの情報をSQSのキューにメッセージとして送信する。

    2. ワーカー（コンシューマ）
        * SQSのキューからメッセージをポーリングで受け取る。
        * メッセージの内容を元に画像のサムネイルを生成する。

### やらないこと
* 下記のような進捗や結果を管理する仕組みは今回は実装しない
    * ワーカーが処理した結果をDBに保存することで、Webアプリ側で処理の進捗や結果を参照できるようになる
    * コールバックURLを指定しておけば、ワーカー完了時に通知する仕組みを作ることができる
* ワーカーの代わりにAWS Lambdaを利用することもできるが、今回はGolangでポーリングするワーカーを作成する

### アーキテクチャ
* 作成するアーキテクチャは以下の通り（AWSのサービスを利用するために、localstackを利用する）
![image](./sqs1.drawio.png)

### 事前準備(AWS CLI v2のインストール)
* [AWS CLI の最新バージョンのインストールまたは更新](https://docs.aws.amazon.com/ja_jp/cli/latest/userguide/getting-started-install.html) に沿ってインストールする

### 事前準備(localstackのインストール)
* 公式にdocker-compose.ymlが用意されているので、それを利用する
    ```yaml
    services:
    localstack:
        container_name: "${LOCALSTACK_DOCKER_NAME:-localstack-main}"
        image: localstack/localstack
        ports:
        - "127.0.0.1:4566:4566"            # LocalStack Gateway
        - "127.0.0.1:4510-4559:4510-4559"  # external services port range
        environment:
        # LocalStack configuration: https://docs.localstack.cloud/references/configuration/
        - DEBUG=${DEBUG:-0}
        volumes:
        - "${LOCALSTACK_VOLUME_DIR:-./volume}:/var/lib/localstack"
        - "/var/run/docker.sock:/var/run/docker.sock"
    ```

* 下記コマンドで起動する
    ```bash
    docker-compose up -d
    ```

* 起動確認(curlにての確認)
    ```bash
    http://localhost:4566/_localstack/health
    # community版で利用できるサービスの一覧を表示
    ```

* 起動確認(localstack Dashboardにての確認)
    ```bash
    https://app.localstack.cloud/inst/default/status
    # ブラウザでアクセスするとStatusで各サービスの状態を確認できる
    ```

### SQSの作成
* localstackを利用してSQSを構築する
    ```bash
    aws --profile localstack sqs create-queue --endpoint-url http://localhost:4566 \
        --queue-name sample-queue \
        --attributes ReceiveMessageWaitTimeSeconds=20
    ```

* SQSの一覧を確認する
    ```bash
    aws --profile localstack --endpoint-url=http://localhost:4566 \
        sqs list-queues
    ```

### S3の作成
* localstackを利用してS3を構築する
    ```bash
    aws --profile localstack --endpoint-url=http://localhost:4566 \
        s3api create-bucket \
        --bucket sample-bucket \
        --create-bucket-configuration LocationConstraint=ap-northeast-1
    ```

* S3の一覧を確認する
    ```bash
    aws --profile localstack --endpoint-url=http://localhost:4566 \
        s3api list-buckets
    ```

### Webアプリ（APIサーバ）の実装
* APIサーバのコードは以下の通り
* `/upload` エンドポイントにリクエストを送信すると、簡易的にS3に画像がアップロードされ、SQSにメッセージが送信される
    ```go
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
    ```

### ワーカー（コンシューマ）の実装
* ワーカーのコードは以下の通り
* ワーカーはSQSのキューからメッセージをポーリングで受け取り、サムネイルを生成してS3にアップロードした後に、メッセージを削除する
    ```go
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
    ```

### 確認テスト
* 下記手順で確認テストを行い、S3上にサムネイル画像が生成されていることを確認した
    ```bash
    # ワーカーを起動
    go run worker.go

    # APIサーバを起動
    go run main.go

    # `/upload` のAPIエンドポイントにリクエストを送信
    curl -X POST http://localhost:8080/upload
    ```

### まとめ
* 今回はサムネイル生成について、SQSを使用した非同期処理を実装した
* サムネイル生成のような、即時反映を必要としない処理については、非同期処理を行うことで、APIのレスポンスを早く返すことができる作りができることを実装を通して確認した
* 今回はAWSのサービスをローカルで利用するために、localstackを利用。S3のバケットのアクセス方式パス式を指定する必要があったところで多少はまったが、community版として、十分な検証ができるサービスだと感じた。
* SQSのポーリング間隔やワーカーの起動タイミング（常駐にするか、必要時に起動するか）など、いくつかのパラメータを調整することで、より柔軟な非同期処理の実装が可能になると思われる。ここは今後詳しくみていきたいポイントである。

### 参考
- [localstack coverage](https://docs.localstack.cloud/references/coverage/)
- https://zenn.dev/kakakakakku/books/aws-application-workshop-using-localstack
- https://zenn.dev/raru_ex/articles/324ae25b04080a
