import { Kafka } from "kafkajs";

const TOPIC_NAME = "zap-events";
const kafka = new Kafka({
    clientId: "outbox-processor",
    brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({ groupId: "zap-group" });

async function main() {
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true })
    // while (true) {
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log({
                    partition,
                    offset: message.offset,
                    value: message.value.toString(),
                });
                await new Promise((resolve) => setTimeout(resolve, 1000))
                console.log("processing done")
                await consumer.commitOffsets([{
                    topic: TOPIC_NAME,
                    partition: partition,
                    offset: (parseInt(message.offset) + 1).toString()
                }])

            },
        });
    // }
}

main();
