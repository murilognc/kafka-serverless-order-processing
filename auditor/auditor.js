const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'auditor',
    brokers: ['kafka:9092']
});

const consumer = kafka.consumer({
    groupId: 'grupo-auditoria'
});

async function run() {

    await consumer.connect();

    await consumer.subscribe({
        topic: 'pedidos-aprovados',
        fromBeginning: true
    });

    await consumer.subscribe({
        topic: 'pedidos-fraude',
        fromBeginning: true
    });

    await consumer.subscribe({
        topic: 'pedidos-analise-manual',
        fromBeginning: true
    });

    console.log('Auditoria iniciada');

    await consumer.run({

        eachMessage: async ({ topic, message }) => {
            console.log('\n========================');
            console.log('EVENTO AUDITORIA');
            console.log('========================');
            console.log('TOPICO:', topic);
            console.log(
                JSON.parse(message.value.toString())
            );
        }
    });
}

run();