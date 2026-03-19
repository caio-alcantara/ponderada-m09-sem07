import http from 'k6/http';
import { check } from 'k6';

const baseUrl = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  scenarios: {
    sensor_data_ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 500,
      maxVUs: 120000,
      stages: [
        { target: 1000, duration: '2m' },
        { target: 5000, duration: '2m' },
        { target: 10000, duration: '2m' },
        { target: 25000, duration: '2m' },
        { target: 50000, duration: '2m' },
        { target: 75000, duration: '2m' },
        { target: 100000, duration: '2m' },
      ],
      gracefulStop: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
  },
};

export default function () {
  const id = Math.floor(Math.random() * 1000000000);

  const payload = JSON.stringify({
    id_dispositivo: id,
    timestamp: new Date().toISOString(),
    tipo_sensor: 'temperatura',
    tipo_leitura: 'analogica',
    valor: 20 + Math.random() * 15,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const response = http.post(`${baseUrl}/sensor_data`, payload, params);

  check(response, {
    'status is 200': (r) => r.status === 200,
  });
}
