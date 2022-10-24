const Kafka = require('node-rdkafka');

console.log('***Consumer starts...***');

const consumer = Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list':'localhost:9092'
}, {});

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'localhost:9092'
},{}, {topic: 'answer'});

function sendAnswer (text) {
    stream.write(Buffer.from(text))
}


consumer.connect();

consumer.on('ready', () => {
    console.log('consumer ready...');
    consumer.subscribe(['task']);
    consumer.consume();
}).on('data', (data) => {
   const message = JSON.parse(data.value);
    if (message.first + message.second == message.third) {
        console.log(`${message.first} + ${message.second} = ${message.third} is TRUE `)
        console.log('answer sent')
        sendAnswer(`That's right, ${message.first} plus ${message.second} is ${message.third} `)
    } else {
        console.log(`${message.first} + ${message.second} = ${message.third} is FAlSE`)
        console.log('answer sent')
        sendAnswer(`Wrong answer, ${message.first} plus ${message.second} is not ${message.third}`)
    }
});





