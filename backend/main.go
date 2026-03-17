package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"

    amqp "github.com/rabbitmq/amqp091-go"
)

type SensorData struct {
    IDDispositivo int64   `json:"id_dispositivo"`
    Timestamp     string  `json:"timestamp"`
    TipoSensor    string  `json:"tipo_sensor"`
    TipoLeitura   string  `json:"tipo_leitura"`
    Valor         float64 `json:"valor"`
}

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

func publishToQueue(payload []byte) error {
    conn, err := amqp.Dial(rabbitMQURL())
    if err != nil {
        return err
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        return err
    }
    defer ch.Close()

    queue, err := ch.QueueDeclare(
        queueName(),
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        return err
    }

    return ch.Publish(
        "",
        queue.Name,
        false,
        false,
        amqp.Publishing{
            ContentType: "application/json",
            Body:        payload,
        },
    )
}

func publishSensorDataAsync(sensorData SensorData) {
    payload, err := json.Marshal(sensorData)
    if err != nil {
        log.Printf("erro ao serializar sensor_data: %v", err)
        return
    }

    go func(data []byte) {
        if err := publishToQueue(data); err != nil {
            log.Printf("erro ao publicar na fila: %v", err)
            return
        }
        log.Printf("mensagem publicada na fila %s", queueName())
    }(payload)
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

    fmt.Printf("sensor_data recebido: %+v\n", sensorData)
    publishSensorDataAsync(sensorData)

    w.WriteHeader(http.StatusOK)
    fmt.Fprint(w, "received")
}

func registerRoutes() {
    http.HandleFunc("/health", healthHandler)
    http.HandleFunc("/sensor_data", sensorDataHandler)
}

func main() {
    println("Server is running on port 8080")
    registerRoutes()
    http.ListenAndServe(":8080", nil)
}