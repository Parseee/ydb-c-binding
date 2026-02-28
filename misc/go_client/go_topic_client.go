package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

const (
	topicName    = "topic"
	consumerName = "consumer"
)

func must0(err error) {
	if err != nil {
		panic(err)
	}
}

func must[R any](res R, err error) R {
	must0(err)
	return res
}

func main() {
	ctx := context.Background()
	db := must(ydb.Open(ctx, "grpc://localhost:2135/?database=/local"))
	createTopic(db)
	// readTopic(db)
}

func createTopic(db *ydb.Driver) {
	ctx := context.Background()
	_ = db.Topic().Drop(ctx, topicName)
	must0(db.Topic().Create(ctx, topicName, topicoptions.CreateWithConsumer(topictypes.Consumer{Name: consumerName})))

	content := make([]byte, 1024)
	for i := range content {
		content[i] = byte(i)
	}

	writer := must(db.Topic().StartWriter(topicName,
		topicoptions.WithWriterWaitServerAck(true),
		topicoptions.WithCodec(topictypes.CodecGzip),
		topicoptions.WithWriterMaxQueueLen(100000),
	))
	for batchIndex := 0; batchIndex < 10; batchIndex++ {
		log.Println(batchIndex)
		var messages []topicwriter.Message

		for i := 0; i < 10000; i++ {
			messages = append(messages, topicwriter.Message{Data: bytes.NewReader(content)})
		}
		must0(writer.Write(ctx, messages...))
	}
	must0(writer.Close(ctx))
}

func readTopic(db *ydb.Driver) {
	ctx := context.Background()
	reader := must(db.Topic().StartReader("consumer", topicoptions.ReadTopic("topic")))
	must(reader.ReadMessage(ctx))
	time.Sleep(time.Second)

	start := time.Now()
	for {
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		batch, err := reader.ReadMessageBatch(readCtx)
		if readCtx.Err() != nil {
			break
		}
		must0(err)
		cancel()

		for i := range batch.Messages {
			io.Copy(io.Discard, batch.Messages[i])
		}
	}
	finish := time.Now()

	duration := finish.Sub(start) - time.Second
	log.Println("time: ", duration)

}
