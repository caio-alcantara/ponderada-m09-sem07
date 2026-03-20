import http from 'k6/http';
import { check } from 'k6';

const baseUrl = __ENV.BASE_URL || 'http://localhost:8080';
const stageDuration = __ENV.STAGE_DURATION || '10s';

export const options = {
  scenarios: {
    sensor_data_ramp_10k: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 500,
      maxVUs: 15000,
      stages: [
        { target: 1000, duration: stageDuration },
        { target: 2000, duration: stageDuration },
        { target: 3000, duration: stageDuration },
        { target: 4000, duration: stageDuration },
        { target: 5000, duration: stageDuration },
        { target: 6000, duration: stageDuration },
        { target: 7000, duration: stageDuration },
        { target: 8000, duration: stageDuration },
        { target: 9000, duration: stageDuration },
        { target: 10000, duration: stageDuration },
      ],
      gracefulStop: '10s',
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
