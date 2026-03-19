package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "strconv"
    "sync"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
)

type SensorData struct {
    IDDispositivo int64   `json:"id_dispositivo"`
    Timestamp     string  `json:"timestamp"`
    TipoSensor    string  `json:"tipo_sensor"`
    TipoLeitura   string  `json:"tipo_leitura"`
    Valor         float64 `json:"valor"`
}

type Publisher struct {
    conn  *amqp.Connection
    queue string
    jobs  chan []byte
    wg    sync.WaitGroup
}

var publisher *Publisher

func rabbitMQURL() string {
    value := os.Getenv("RABBITMQ_URL")
    if value == "" {
        return "amqp://guest:guest@localhost:5672/"
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

func envInt(name string, defaultValue int) int {
    value := os.Getenv(name)
    if value == "" {
        return defaultValue
    }

    parsed, err := strconv.Atoi(value)
    if err != nil || parsed <= 0 {
        return defaultValue
    }

    return parsed
}

func workerCount() int {
    return envInt("PUBLISH_WORKERS", 8)
}

func publishBufferSize() int {
    return envInt("PUBLISH_BUFFER", 20000)
}

func newPublisher() (*Publisher, error) {
    conn, err := amqp.Dial(rabbitMQURL())
    if err != nil {
        return nil, err
    }

    setupChannel, err := conn.Channel()
    if err != nil {
        conn.Close()
        return nil, err
    }
    defer setupChannel.Close()

    queue, err := setupChannel.QueueDeclare(
        queueName(),
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        conn.Close()
        return nil, err
    }

    newPublisher := &Publisher{
        conn:  conn,
        queue: queue.Name,
        jobs:  make(chan []byte, publishBufferSize()),
    }

    workers := workerCount()
    for index := 0; index < workers; index++ {
        workerChannel, err := conn.Channel()
        if err != nil {
            conn.Close()
            return nil, err
        }

        newPublisher.wg.Add(1)
        go func(channel *amqp.Channel) {
            defer newPublisher.wg.Done()
            defer channel.Close()

            for payload := range newPublisher.jobs {
                publishContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
                err := channel.PublishWithContext(
                    publishContext,
                    "",
                    newPublisher.queue,
                    false,
                    false,
                    amqp.Publishing{
                        ContentType: "application/json",
                        Body:        payload,
                    },
                )
                cancel()

                if err != nil {
                    log.Printf("erro ao publicar na fila: %v", err)
                }
            }
        }(workerChannel)
    }

    return newPublisher, nil
}

func (publisher *Publisher) enqueue(payload []byte) bool {
    select {
    case publisher.jobs <- payload:
        return true
    default:
        return false
    }
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }

    w.WriteHeader(http.StatusOK)
    fmt.Fprint(w, "ok")
}

func sensorDataHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }

    var sensorData SensorData
    if err := json.NewDecoder(r.Body).Decode(&sensorData); err != nil {
        http.Error(w, "invalid json", http.StatusBadRequest)
        return
    }

    payload, err := json.Marshal(sensorData)
    if err != nil {
        http.Error(w, "invalid payload", http.StatusBadRequest)
        return
    }

    if !publisher.enqueue(payload) {
        http.Error(w, "queue overloaded", http.StatusServiceUnavailable)
        return
    }

    w.WriteHeader(http.StatusOK)
    fmt.Fprint(w, "received")
}

func registerRoutes() {
    http.HandleFunc("/health", healthHandler)
    http.HandleFunc("/sensor_data", sensorDataHandler)
}

func main() {
    initializedPublisher, err := newPublisher()
    if err != nil {
        log.Fatalf("erro ao iniciar publisher: %v", err)
    }
    publisher = initializedPublisher

    println("Server is running on port 8080")
    registerRoutes()
    log.Fatal(http.ListenAndServe(":8080", nil))
}