package main

import (
    "encoding/json"
    "fmt"
    "net/http"
)

type SensorData struct {
    IDDispositivo int64   `json:"id_dispositivo"`
    Timestamp     string  `json:"timestamp"`
    TipoSensor    string  `json:"tipo_sensor"`
    TipoLeitura   string  `json:"tipo_leitura"`
    Valor         float64 `json:"valor"`
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