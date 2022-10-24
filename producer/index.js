// import Kafka from 'node-rdkafka;'
const Kafka = require('node-rdkafka');
console.log('***Producer starts...***')

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list':'localhost:9092'
}, {}, {topic: 'task'});


const consumer = Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list':'localhost:9092'
}, {});



function randomixeIntegerBetween(from, to) {
    return (Math.floor(Math.random() * (to - from))) + from;
}

function queueMessage() {
    const o1 = randomixeIntegerBetween(1,3);
    const o2 = randomixeIntegerBetween(1,3);
    const o3 = randomixeIntegerBetween(2,4);

    const data = {
        first: o1,
        second: o2,
        third: o3
    }

    const success = stream.write(Buffer.from(
       // `is this correct ${o1} + ${o2} = ${o3}?`
      JSON.stringify(data)
    ));

    if (success) {
        console.log('Message sent');
    } else {
        console.log('Houston, we have a problem..')
    }
}

consumer.connect();

consumer.on('ready', () => {
    console.log('producer-consumer ready...');
    consumer.subscribe(['answer']);
    consumer.consume();
}).on('data', (data) => {
    console.log('receiving message')
    console.log(`message: ${data.value}`);
});



setInterval(() => {
    queueMessage();
}, 2500)