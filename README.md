# 🇺🇸 Adapters

Projeto com diversos _Adapters_ para se integrar com _Cache_, _PubSub_, servidores e clientes **HTTP**, camada de _log_, etc.,
no intuito de permitir que um projeto que faça uso deste possa conter apenas códigos de regra de negócio, particulares a ele.

## Lista de especificações e implementações disponíveis

*  Integração com variáveis de ambiente¹
    *  Variáveis do sistema
    *  [Vault](https://www.vaultproject.io/)
*  Log
    *  [logrus](https://github.com/sirupsen/logrus)
*  Database*
    *  [go-mssqldb](https://github.com/denisenkom/go-mssqldb)
    *  [mongo-driver](https://go.mongodb.org/mongo-driver)
*  Cache
    *  [redigo](https://github.com/gomodule/redigo)
*  Server HTTP
    *  net/http + [gorilla/mux](https://github.com/gorilla/mux)
    *  [fasthttp](https://github.com/valyala/fasthttp) + [fasthttp-routing](https://github.com/qiangxue/fasthttp-routing)
*  Cliente HTTP
    *  [fasthttp](https://github.com/valyala/fasthttp)
*  Publisher
    *  Kafka
        *  [kafka-go](https://github.com/segmentio/kafka-go)
    *  RabbitMQ
        *  [amqp](https://github.com/streadway/amqp)
*  Subscriber
    *  Kafka
        *  [kafka-go](https://github.com/segmentio/kafka-go) + [backoff](https://github.com/cenkalti/backoff) (para retentativa de acknowledge)
    *  RabbitMQ
        *  [amqp](https://github.com/streadway/amqp) + [backoff](https://github.com/cenkalti/backoff) (para retentativa de acknowledge)

*Possui apenas especificação de como se conectar com o banco utilizando estes _drives_.