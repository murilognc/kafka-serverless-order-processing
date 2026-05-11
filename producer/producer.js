const { Kafka } = require('kafkajs');

const { cpf } = require('cpf-cnpj-validator');

const kafka = new Kafka({
    clientId: 'producer',
    brokers: ['kafka:9092']
});

const producer = kafka.producer();

async function run() {

    await producer.connect();

    let contador = 1;

    setInterval(async () => {
        const pedido = {
            id: contador,
            cliente: 'Cliente ' + contador,
            cpf:
                contador % 5 === 0
                    ? '12345678900'
                    : cpf.generate(),
            valor: Math.floor(Math.random() * 2000),
            itens: [
                {
                    produto: 'Notebook',
                    quantidade: 1
                }
            ],
            criadoEm: new Date()
        };

        await producer.send({
            topic: 'pedidos',
            messages: [
                {
                    value: JSON.stringify(pedido)
                }
            ]
        });
        console.log('\nPEDIDO ENVIADO');
        console.log(pedido);
        contador++;
    }, 5000);
}

run();