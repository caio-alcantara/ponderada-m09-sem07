package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SensorData struct {
	IDDispositivo int64   `json:"id_dispositivo"`
	Timestamp     string  `json:"timestamp"`
	TipoSensor    string  `json:"tipo_sensor"`
	TipoLeitura   string  `json:"tipo_leitura"`
	Valor         float64 `json:"valor"`
}

type pendingItem struct {
	delivery amqp.Delivery
	data     SensorData
	ts       time.Time
}

func rabbitMQURL() string {
	value := os.Getenv("RABBITMQ_URL")
	if value == "" {
		return "amqp://guest:guest@rabbitmq:5672/"
	}
	return value
}

func queueName() string {
	value := os.Getenv("RABBITMQ_QUEUE")
	if value == "" {
		return "sensor_data_queue"
	}
	return value
}

func postgresDSN() string {
	value := os.Getenv("POSTGRES_DSN")
	if value == "" {
		return "host=postgres port=5432 user=postgres password=postgres dbname=sensores sslmode=disable"
	}
	return value
}

func batchSize() int {
	value := os.Getenv("BATCH_SIZE")
	if value == "" {
		return 5000
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 5000
	}
	if parsed > 5000 {
		return 5000
	}
	return parsed
}

func batchFlushInterval() time.Duration {
	value := os.Getenv("BATCH_FLUSH_INTERVAL")
	if value == "" {
		return 2 * time.Second
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 2 * time.Second
	}
	return parsed
}

func connectPostgres() (*sql.DB, error) {
	db, err := sql.Open("postgres", postgresDSN())
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func connectRabbit(size int) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery, error) {
	conn, err := amqp.Dial(rabbitMQURL())
	if err != nil {
		return nil, nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, nil, err
	}

	_, err = ch.QueueDeclare(
		queueName(),
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, nil, err
	}

	if err := ch.Qos(size, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, nil, err
	}

	msgs, err := ch.Consume(
		queueName(),
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, nil, err
	}

	return conn, ch, msgs, nil
}

func parseTimestamp(value string) (time.Time, error) {
	layouts := []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05"}
	for _, layout := range layouts {
		timestamp, err := time.Parse(layout, value)
		if err == nil {
			return timestamp, nil
		}
	}
	return time.Time{}, fmt.Errorf("timestamp inválido: %s", value)
}

func processBatch(db *sql.DB, batch []amqp.Delivery) {
	pending := make([]pendingItem, 0, len(batch))
	for _, message := range batch {
		var sensorData SensorData
		if err := json.Unmarshal(message.Body, &sensorData); err != nil {
			log.Printf("mensagem inválida (json): %v", err)
			if err := message.Ack(false); err != nil {
				log.Printf("erro ao ack mensagem inválida: %v", err)
			}
			continue
		}

		ts, err := parseTimestamp(sensorData.Timestamp)
		if err != nil {
			log.Printf("mensagem inválida (timestamp): %v", err)
			if err := message.Ack(false); err != nil {
				log.Printf("erro ao ack mensagem inválida: %v", err)
			}
			continue
		}

		pending = append(pending, pendingItem{delivery: message, data: sensorData, ts: ts})
	}

	if len(pending) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("erro ao iniciar transação: %v", err)
		nackPending(pending)
		return
	}

	statement, err := tx.PrepareContext(ctx, `
		INSERT INTO leituras_sensores (id_dispositivo, "timestamp", tipo_sensor, tipo_leitura, valor)
		VALUES ($1, $2, $3, $4, $5)
	`)
	if err != nil {
		log.Printf("erro ao preparar insert: %v", err)
		tx.Rollback()
		nackPending(pending)
		return
	}
	defer statement.Close()

	for _, item := range pending {
		_, err := statement.ExecContext(
			ctx,
			item.data.IDDispositivo,
			item.ts,
			item.data.TipoSensor,
			item.data.TipoLeitura,
			item.data.Valor,
		)
		if err != nil {
			log.Printf("erro ao inserir lote: %v", err)
			tx.Rollback()
			nackPending(pending)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("erro ao commitar lote: %v", err)
		nackPending(pending)
		return
	}

	for _, item := range pending {
		if err := item.delivery.Ack(false); err != nil {
			log.Printf("erro ao ack mensagem: %v", err)
		}
	}

	log.Printf("lote salvo com sucesso: %d mensagens", len(pending))
}

func nackPending(pending []pendingItem) {
	for _, item := range pending {
		if err := item.delivery.Nack(false, true); err != nil {
			log.Printf("erro ao nack mensagem: %v", err)
		}
	}
}

func consumeLoop(db *sql.DB, msgs <-chan amqp.Delivery, size int, flushEvery time.Duration) error {
	ticker := time.NewTicker(flushEvery)
	defer ticker.Stop()

	batch := make([]amqp.Delivery, 0, size)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		processBatch(db, batch)
		batch = batch[:0]
	}

	for {
		select {
		case message, ok := <-msgs:
			if !ok {
				flush()
				return fmt.Errorf("canal de consumo fechado")
			}

			batch = append(batch, message)
			if len(batch) >= size {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func run() error {
	size := batchSize()
	flushEvery := batchFlushInterval()

	db, err := connectPostgres()
	if err != nil {
		return fmt.Errorf("erro postgres: %w", err)
	}
	defer db.Close()

	conn, ch, msgs, err := connectRabbit(size)
	if err != nil {
		return fmt.Errorf("erro rabbitmq: %w", err)
	}
	defer ch.Close()
	defer conn.Close()

	log.Printf("consumindo fila %s com lote até %d mensagens", queueName(), size)
	return consumeLoop(db, msgs, size, flushEvery)
}

func main() {
	log.Println("middleware iniciado")
	for {
		if err := run(); err != nil {
			log.Printf("erro no middleware: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
}
