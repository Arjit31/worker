import {Kafka} from "kafkajs"

const TOPIC_NAME = "zap-events"
const kafka = new Kafka({
  clientId: 'outbox-processor',
  brokers: ['localhost:9092']
})
const consumer = kafka.consumer({ groupId: 'zap-group' })


async function main(){
    while(true){

    }
}

main()