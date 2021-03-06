group_prometheus:
  group:
    - present
    - name: prometheus
    - system: True

user_prometheus:
  user:
    - present
    - name: prometheus
    - groups:
      - prometheus
    - home: /opt/prometheus
    - createhome: False
    - shell: /bin/false
    - system: True