const { Kafka } = require('kafkajs');

const axios = require('axios');

const kafka = new Kafka({
    clientId: 'consumer-trigger',
    brokers: ['kafka:9092']
});

const consumer = kafka.consumer({
    groupId: 'grupo-trigger'
});

const admin = kafka.admin();

async function criarTopicos() {

    await admin.connect();

    await admin.createTopics({
        topics: [
            {
                topic: 'pedidos',
                numPartitions: 1,
                replicationFactor: 1
            },
            {
                topic: 'pedidos-aprovados',
                numPartitions: 1,
                replicationFactor: 1
            },
            {
                topic: 'pedidos-fraude',
                numPartitions: 1,
                replicationFactor: 1
            },
            {
                topic: 'pedidos-analise-manual',
                numPartitions: 1,
                replicationFactor: 1
            }
        ]
    });

    console.log('Topicos criados');

    await admin.disconnect();
}

async function run() {

    await criarTopicos();

    await consumer.connect();

    await consumer.subscribe({
        topic: 'pedidos',
        fromBeginning: true
    });

    console.log('Trigger iniciado');

    await consumer.run({

        eachMessage: async ({ message }) => {

            const pedido = JSON.parse(
                message.value.toString()
            );

            console.log('\nEVENTO RECEBIDO');
            console.log(pedido);

            // CHAMA FAAS
            const response = await axios.post(
                'http://function:3000/function/processar',
                pedido
            );

            console.log('\nFAAS RESPONDEU');
            console.log(response.data);
        }
    });
}

run().catch(console.error);