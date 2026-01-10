"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const client_1 = require("./generated/prisma/client");
const adapter_pg_1 = require("@prisma/adapter-pg");
const kafkajs_1 = require("kafkajs");
const dotenv_1 = __importDefault(require("dotenv"));
const mustache_1 = __importDefault(require("mustache"));
const email_1 = require("./email");
const gemini_1 = require("./gemini");
const fs = __importStar(require("fs"));
const path_1 = __importDefault(require("path"));
const express_1 = __importDefault(require("express"));
const cors_1 = __importDefault(require("cors"));
dotenv_1.default.config();
const app = (0, express_1.default)();
const PORT = process.env.PORT || 3005;
const corsOptions = {
    credentials: true,
    origin: process.env.FRONTEND_URL,
};
app.use((0, cors_1.default)(corsOptions));
app.get("/", (req, res) => {
    console.log("pinged");
    res.status(200).send("OK");
});
app.listen(PORT, () => {
    console.log(`Ping server running on ${PORT}`);
});
const adapter = new adapter_pg_1.PrismaPg({
    connectionString: process.env.DATABASE_URL,
});
const prisma = new client_1.PrismaClient({ adapter });
const TOPIC_NAME = "zap-events";
// const kafka = new Kafka({
//     clientId: "outbox-processor",
//     brokers: ["localhost:9092"],
// });
const kafka = new kafkajs_1.Kafka({
    clientId: "outbox-processor",
    brokers: ["kafka-378cf09f-arjit-chat-db.l.aivencloud.com:20666"],
    ssl: {
        key: fs.readFileSync(process.env.ENVIRONMET === "DEV"
            ? path_1.default.join(__dirname, "../certs/service.key")
            : path_1.default.join(__dirname, "../../certs/service.key"), "utf-8"),
        cert: fs.readFileSync(process.env.ENVIRONMET === "DEV"
            ? path_1.default.join(__dirname, "../certs/service.cert") :
            path_1.default.join(__dirname, "../../certs/service.cert"), "utf-8"),
        ca: [
            fs.readFileSync(process.env.ENVIRONMET === "DEV"
                ? path_1.default.join(__dirname, "../certs/ca.pem") :
                path_1.default.join(__dirname, "../../certs/ca.pem"), "utf-8"),
        ],
    },
});
const consumer = kafka.consumer({ groupId: "zap-group" });
const producer = kafka.producer();
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        yield consumer.connect();
        yield producer.connect();
        yield consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
        yield consumer.run({
            eachMessage: (_a) => __awaiter(this, [_a], void 0, function* ({ topic, partition, message }) {
                var _b;
                try {
                    console.log({
                        partition,
                        offset: message.offset,
                        value: (_b = message.value) === null || _b === void 0 ? void 0 : _b.toString(),
                    });
                    if (!message.value) {
                        console.log("processing done");
                        CommitMessage(partition, message);
                        return;
                    }
                    const parsedValue = JSON.parse(message.value.toString());
                    const zapRunId = parsedValue.zapRunId;
                    const stage = parsedValue.stage;
                    const prevMetadata = parsedValue.prevMetadata;
                    const zapRunDetails = yield prisma.zapRun.findFirst({
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
                    const currAction = zapRunDetails.zap.actions.find((x) => x.sortOrder === stage);
                    const metadata = currAction === null || currAction === void 0 ? void 0 : currAction.metadata;
                    let response = "";
                    if (!currAction || !metadata) {
                        console.log("Action or metadata not found");
                        return;
                    }
                    const zapRunMetadata = zapRunDetails.metadata;
                    const newMetadata = Object.assign(Object.assign({}, zapRunMetadata), prevMetadata);
                    if (currAction.type.name === "Email") {
                        try {
                            const to = mustache_1.default.render(metadata === null || metadata === void 0 ? void 0 : metadata.to, newMetadata);
                            let body = mustache_1.default.render(metadata === null || metadata === void 0 ? void 0 : metadata.body, newMetadata);
                            let subject = mustache_1.default.render(metadata === null || metadata === void 0 ? void 0 : metadata.subject, newMetadata);
                            console.log("sending email");
                            (0, email_1.sendEmail)(to, body, subject);
                        }
                        catch (error) {
                            console.log(error);
                            console.log("processing done");
                            CommitMessage(partition, message);
                            return;
                        }
                    }
                    else if (currAction.type.name === "Gemini") {
                        try {
                            const question = mustache_1.default.render(metadata === null || metadata === void 0 ? void 0 : metadata.question, newMetadata);
                            console.log("Getting Gemini Response");
                            response = "" + (yield (0, gemini_1.geminiResponse)(question));
                        }
                        catch (error) {
                            console.log(error);
                            console.log("processing done");
                            CommitMessage(partition, message);
                            return;
                        }
                    }
                    yield new Promise((resolve) => setTimeout(resolve, 1000));
                    const lastStage = (zapRunDetails.zap.actions.length || 1) - 1;
                    const responseInd = "response" + (stage + 2);
                    prevMetadata[responseInd] = response;
                    if (stage !== lastStage) {
                        yield producer.send({
                            topic: TOPIC_NAME,
                            messages: [
                                {
                                    value: JSON.stringify({
                                        zapRunId: zapRunId,
                                        stage: stage + 1,
                                        prevMetadata: prevMetadata,
                                    }),
                                },
                            ],
                        });
                    }
                    console.log("processing done");
                    CommitMessage(partition, message);
                }
                catch (error) {
                    console.log(error);
                }
            }),
        });
    });
}
main();
function CommitMessage(partition, message) {
    return __awaiter(this, void 0, void 0, function* () {
        yield consumer.commitOffsets([
            {
                topic: TOPIC_NAME,
                partition: partition,
                offset: (parseInt(message.offset) + 1).toString(),
            },
        ]);
    });
}
