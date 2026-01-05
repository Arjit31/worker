import { PrismaClient } from "./generated/prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Kafka, KafkaMessage } from "kafkajs";
import dotenv from "dotenv";
import { JsonObject } from "@prisma/client/runtime/client";
import Mustache from "mustache";
import { sendEmail } from "./email";
import { geminiResponse } from "./gemini";
dotenv.config();

const adapter = new PrismaPg({
    connectionString: process.env.DATABASE_URL,
});

const prisma = new PrismaClient({ adapter });

const TOPIC_NAME = "zap-events";
const kafka = new Kafka({
    clientId: "outbox-processor",
    brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({ groupId: "zap-group" });
const producer = kafka.producer();

async function main() {
    await consumer.connect();
    await producer.connect();
    await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
    // while (true) {
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                partition,
                offset: message.offset,
                value: message.value?.toString(),
            });
            if (!message.value) {
                console.log("processing done");
                CommitMessage(partition, message);
                return;
            }
            const parsedValue = JSON.parse(message.value.toString());
            const zapRunId = parsedValue.zapRunId;
            const stage = parsedValue.stage;
            const prevMetadata = parsedValue.prevMetadata as JsonObject;

            const zapRunDetails = await prisma.zapRun.findFirst({
                where: {
                    id: zapRunId,
                },
                include: {
                    zap: {
                        include: {
                            actions: {
                                include: {
                                    type: true,
                                },
                            },
                        },
                    },
                },
            });

            console.log(zapRunDetails);

            if (!zapRunDetails) {
                console.log("processing done");
                CommitMessage(partition, message);
                return;
            }

            const currAction = zapRunDetails.zap.actions.find(
                (x) => x.sortOrder === stage
            );
            const metadata = currAction?.metadata as JsonObject;
            let response = "";
            if (!currAction || !metadata) {
                console.log("Action or metadata not found");
                return;
            }
            const zapRunMetadata = zapRunDetails.metadata as JsonObject;
            const newMetadata = {...zapRunMetadata, ...prevMetadata}
            if (currAction.type.name === "Email") {
                try {
                    const to = Mustache.render(metadata?.to as string, newMetadata);
                    let body = Mustache.render(metadata?.body as string, newMetadata);
                    // console.log(body, metadata?.body);
                    // body = Mustache.render(body, prevMetadata);
                    console.log(prevMetadata);
                    console.log(newMetadata);
                    console.log(to);
                    console.log(body);
                    console.log("sending email");
                    sendEmail(to, body);
                } catch (error) {
                    console.log(error);
                    console.log("processing done");
                    CommitMessage(partition, message);
                    return;
                }
            } else if (currAction.type.name === "Gemini") {
                try {
                    const question = Mustache.render(metadata?.question as string, newMetadata);
                    console.log(newMetadata);
                    console.log(question);
                    console.log("Getting Gemini Response");
                    response = "" + await geminiResponse(question)
                } catch (error) {
                    console.log(error);
                    console.log("processing done");
                    CommitMessage(partition, message);
                    return;
                }
            }

            await new Promise((resolve) => setTimeout(resolve, 1000));

            const lastStage = (zapRunDetails.zap.actions.length || 1) - 1;
            const responseInd = "response"+(stage+1)
            prevMetadata[responseInd] = response;
            if (stage !== lastStage) {
                await producer.send({
                    topic: TOPIC_NAME,
                    messages: [
                        {
                            value: JSON.stringify({
                                zapRunId: zapRunId,
                                stage: stage + 1,
                                prevMetadata: prevMetadata
                            }),
                        },
                    ],
                });
            }

            console.log("processing done");
            CommitMessage(partition, message);
        },
    });
    // }
}

main();

async function CommitMessage(partition: number, message: KafkaMessage) {
    await consumer.commitOffsets([
        {
            topic: TOPIC_NAME,
            partition: partition,
            offset: (parseInt(message.offset) + 1).toString(),
        },
    ]);
}
