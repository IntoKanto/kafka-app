// import Kafka from 'node-rdkafka;'
const Kafka = require('node-rdkafka');
console.log('***Producer starts...***')

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list':'localhost:9092'
}, {}, {topic: 'task'});

function randomixeIntegerBetween(from, to) {
    return (Math.floor(Math.random() * (to - from))) + from;
}

function queueMessage() {
    const o1 = randomixeIntegerBetween(1,10);
    const o2 = randomixeIntegerBetween(1,10);
    const o3 = randomixeIntegerBetween(2,10);

    const success = stream.write(Buffer.from(
        `is this correct ${o1} + ${o2} = ${o3}?`
    ));

    if (success) {
        console.log('Message successfully to stream');
    } else {
        console.log('Problem writing to stream..')
    }
}

setInterval(() => {
    queueMessage();
}, 2500)