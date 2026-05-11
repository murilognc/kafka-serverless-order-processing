# Aula Prática — Arquitetura Event-Driven com Kafka + FaaS + Docker

## Objetivo

Esta prática demonstra uma arquitetura baseada em eventos utilizando:

* Apache Kafka, Funções Serverless (FaaS), Docker Compose
* Node.js, Processamento assíncrono e Pipeline de eventos

A proposta é simular um fluxo real de processamento de pedidos de e-commerce.

---

# Pré-requisitos

* Docker Desktop
* Docker Compose

---

# Executando o Projeto

## 1. Clonar o projeto

```bash
git clone <repositorio>
```

---

## 2. Entrar na pasta

```bash
cd kafka-serverless-order-processing
```

---

## 3. Subir containers

```bash
docker compose up --build
```

---

# Limpando Ambiente

Caso ocorram erros de cache ou mensagens antigas:

```bash
docker compose down -v
```

```bash
docker system prune -af
```

Depois execute novamente:

```bash
docker compose up --build
```

---

# Arquitetura

```text
Producer
   ↓
Kafka (pedidos)
   ↓
Consumer Trigger
   ↓ HTTP
FaaS (processamento antifraude)
   ↓
├── pedidos-aprovados
├── pedidos-fraude
└── pedidos-analise-manual
   ↓
Auditor
```

---

# Componentes

## Producer

Responsável por:

* gerar pedidos automaticamente
* produzir eventos no Kafka
* simular clientes reais

Os pedidos possuem:

* id
* cliente
* CPF
* valor
* itens
* data de criação

---

## Kafka

Responsável pelo transporte assíncrono de eventos.

Tópicos utilizados:

| Tópico                 | Finalidade                   |
| ---------------------- | ---------------------------- |
| pedidos                | Entrada principal de pedidos |
| pedidos-aprovados      | Pedidos aprovados            |
| pedidos-fraude         | Pedidos fraudulentos         |
| pedidos-analise-manual | Pedidos suspeitos            |

---

## Consumer Trigger

Responsável apenas por:

* consumir eventos do tópico `pedidos`
* chamar a FaaS via HTTP

Importante:

O consumer NÃO contém regras de negócio.

Toda lógica está centralizada na FaaS.

---

## FaaS

A função serverless executa:

* validação de CPF
* classificação de risco
* análise antifraude
* roteamento de eventos

Regras implementadas:

| Regra        | Resultado              |
| ------------ | ---------------------- |
| CPF inválido | pedidos-fraude         |
| Valor > 1000 | pedidos-analise-manual |
| Restante     | pedidos-aprovados      |

---

## Auditor

Serviço responsável por:

* consumir eventos finais
* exibir resultados
* monitorar o pipeline

---

# Estrutura do Projeto

```text
.
├── docker-compose.yml
├── producer/
│   ├── Dockerfile
│   ├── package.json
│   └── producer.js
│
├── consumer/
│   ├── Dockerfile
│   ├── package.json
│   └── consumer.js
│
├── function/
│   ├── Dockerfile
│   ├── package.json
│   └── handler.js
│
└── auditor/
    ├── Dockerfile
    ├── package.json
    └── auditor.js
```

---

# Tecnologias Utilizadas

* Node.js
* Express
* KafkaJS
* Apache Kafka
* Docker
* Docker Compose
* cpf-cnpj-validator

---

# Fluxo de Execução

## 1. Producer envia pedidos

Exemplo:

```text
PEDIDO ENVIADO
```

---

## 2. Consumer Trigger recebe evento

Exemplo:

```text
EVENTO RECEBIDO
```

---

## 3. FaaS processa pedido

Valida:

* CPF
* valor
* risco

---

## 4. FaaS publica resultado

Exemplo:

```text
EVENTO PUBLICADO
```

---

## 5. Auditor exibe resultado final

Exemplo:

```text
TOPICO: pedidos-aprovados
```
---

# Exemplo de Pedido

```json
{
  "id": 1,
  "cliente": "Cliente 1",
  "cpf": "12345678909",
  "valor": 850,
  "itens": [
    {
      "produto": "Notebook",
      "quantidade": 1
    }
  ]
}
```

---

# Exemplo de Evento Processado

```json
{
  "pedidoId": 1,
  "cliente": "Cliente 1",
  "status": "pedidos-aprovados",
  "motivo": "Pedido aprovado"
}
```

---

# Possíveis Evoluções

* Persistência em PostgreSQL
* Retry automático
* Dead Letter Queue (DLQ)
* Kafka UI
* Prometheus
* Grafana
* API Gateway
* Autenticação
* Escalabilidade horizontal
* OpenFaaS real com Kubernetes

---

# Autor

Professor: Murilo Gustavo Nabarrete Costa

Projeto desenvolvido para fins educacionais da disciplina de Computação em Nuvem