const express = require('express');

const { Kafka } = require('kafkajs');

const { cpf } = require('cpf-cnpj-validator');

const app = express();

app.use(express.json());

const kafka = new Kafka({
    clientId: 'faas-antifraude',
    brokers: ['kafka:9092']
});

const producer = kafka.producer();

async function enviarEvento(topic, pedido, motivo) {

    const evento = {
        pedidoId: pedido.id,
        cliente: pedido.cliente,
        cpf: pedido.cpf,
        valor: pedido.valor,
        status: topic,
        motivo,
        processadoEm: new Date()
    };

    await producer.send({
        topic,
        messages: [
            {
                value: JSON.stringify(evento)
            }
        ]
    });

    console.log('\nEVENTO PUBLICADO');
    console.log(topic);
    console.log(evento);
}

async function iniciarKafka() {
    await producer.connect();
    console.log('Kafka conectado na FaaS');
}

iniciarKafka();

app.post('/function/processar', async (req, res) => {

    const pedido = req.body;

    console.log('\n========================');
    console.log('PROCESSANDO PEDIDO');
    console.log('========================');

    console.log(pedido);

    // REGRA 1
    // CPF inválido
    if (!cpf.isValid(pedido.cpf)) {

        await enviarEvento(
            'pedidos-fraude',
            pedido,
            'CPF inválido'
        );

        return res.json({
            status: 'fraude'
        });
    }

    // REGRA 2
    // Valor alto
    if (pedido.valor > 1000) {
        await enviarEvento(
            'pedidos-analise-manual',
            pedido,
            'Valor alto'
        );
        return res.json({
            status: 'analise-manual'
        });
    }

    // REGRA 3
    // Aprovado
    await enviarEvento(
        'pedidos-aprovados',
        pedido,
        'Pedido aprovado'
    );
    res.json({
        status: 'aprovado'
    });
});

app.listen(3000, () => {
    console.log('FaaS rodando');
});