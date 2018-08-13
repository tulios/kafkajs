/**
 * Constructor types
 */

interface SSLConfiguration {
    rejectUnauthorized: boolean;
    ca: string[];
    key: string;
    cert: string;
}
export type SSLOptions = SSLConfiguration | boolean;
export interface SASLOptions {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512';
    username: string;
    password: string;
}
export interface RetryOptions {
    maxRetryTime?: number;
    initialRetryTime?: number;
    factor?: number;
    multiplier?: number;
    retries?: number;
}
export enum ELogLevel {
    NOTHING = 0,
    ERROR = 1,
    WARN = 2,
    INFO = 4,
    DEBUG = 5,
}
export interface LoggerPayload {
    namespace: string;
    level: ELogLevel;
    label: string;
    log: {
        timestamp: string;
        logger: string;
        message: string;
    };
}
export type Logger = (payload: LoggerPayload) => void
export type LogCreator = (logLevel: ELogLevel) => Logger
export interface ConstructorOptions {
    clientId: string;
    brokers: string[];
    connectionTimeout?: number;
    authenticationTimeout?: number;
    ssl?: SSLOptions;
    sasl?: SASLOptions;
    retry?: RetryOptions;
    logLevel?: ELogLevel;
    logCreator?: LogCreator;
}

/**
 * Producer
 */
interface ProducerMessage {
    key?: string;
    value: string | Buffer;
    partition?: number;
}
export enum ECompressionTypes {
    None = 0,
    GZIP = 1,
    Snappy = 2,
    LZ4 = 3,
}
interface BaseProducerSendPayload {
    acks?: -1 | 0 | 1;
    timeout?: number;
    compression?: ECompressionTypes
}
interface ProducerSendPayload extends BaseProducerSendPayload {
    topic: string;
    messages: ProducerMessage[]
}
interface ProducerSendBatchPayload extends BaseProducerSendPayload {
    topicMessages: ProducerSendPayload[];
}
export interface PartitionMetadata {
    partitionId: number;
    leader: number;
}
export interface PartitionerCreatorOptions {
    topic: string;
    partitionMetadata: PartitionMetadata;
    message: ProducerMessage;
}
export type PartitionerCreator = () => (options: PartitionerCreatorOptions) => number
export interface ProducerOptions {
    createPartitioner?: PartitionerCreator
    metadataMaxAge?: number;
    retry?: RetryOptions
}
export class Producer {
    connect(): Promise<void>;
    send(payload: ProducerSendPayload): Promise<void>;
    disconnect(): Promise<void>;
    sendBatch(payload: ProducerSendBatchPayload): Promise<void>;
}

/**
 * Consumer
 */
export interface ConsumerMessage {
    key?: any;
    value: string | Buffer;
    offset: number;
}
export interface SubscribePayload {
    topic: string;
    fromBeginning?: boolean;
}
export interface EachMessagePayload {
    topic: string;
    partition: number;
    message: ConsumerMessage;
}
export interface Batch {
    highWatermark: number;
    topic: string;
    partition: number;
    messages: ConsumerMessage[];
}
export interface EachBatchPayload {
    batch: Batch;
    resolveOffset: (offset: number) => Promise<void>;
    heartbeat: () => Promise<void>;
    isRunning: () => boolean;
}
export interface RunOptions {
    eachBatchAutoResolve?: boolean;
    commitOffsetsIfNecessary?: boolean;
    autoCommitInterval?: number;
    autoCommitThreshold?: number;
    eachMessage?: (payload: EachMessagePayload) => Promise<void>;
    eachBatch?: (payload: EachBatchPayload) => Promise<void>;
}
export interface ConsumerOptions {
    groupId?: string;
    partitionAssigners?: any[];
    sessionTimeout?: number;
    heartbeatInterval?: number;
    metadataMaxAge?: number;
    maxBytesPerPartition?: number;
    minBytes?: number;
    maxBytes?: number;
    maxWaitTimeInMs?: number;
    retry?: RetryOptions;
}
export interface PauseResumePayload {
    topic: string;
}
export interface SeekPayload {
    topic: string;
    partition: number;
    offset: string | number;
}
export class Consumer {
    connect(options?: ConsumerOptions): Promise<void>;
    disconnect(): Promise<void>;
    subscribe(payload: SubscribePayload): Promise<void>;
    run(options: RunOptions): Promise<void>;
    pause(payload?: PauseResumePayload): void;
    resume(payload?: PauseResumePayload): void;
    seek(payload: SeekPayload): void;
}

/**
 * Admin
 */
export interface AdminOptions {
    retry: RetryOptions;

}
export interface ReplicaAssignment {
    partition: number;
    replicas: number[];
}
export interface ConfigEntry {
    name: string;
    value: string | number;
}
export interface Topic {
    topic: string;
    numPartitions?: number;
    replicationFactor?: number;
    replicaAssignment?: ReplicaAssignment[];
    configEntries?: ConfigEntry[];
}
export interface CreateTopicsPayload {
    validateOnly?: boolean;
    timeout?: number;
    waitForLeaders?: boolean;
    topics: Topic[];
}
export interface FetchOffsetsPayload {
    groupId: string;
    topic: string;
}
export interface Offset {
    partition: number;
    offset: string;
}
export interface ResetOffsetsPayload {
    groupId: string;
    topic: string;
    earliest?: boolean;
}
export interface Partition {
    partition: number;
    offset: string | number;
}
export interface SetOffsetsPayload {
    groupId: string;
    topic: string;
    partitions: Partition[];
}
export class Admin {
    connect(): Promise<void>
    disconnect(): Promise<void>;
    createTopics(payload: CreateTopicsPayload): Promise<void>;
    fetchOffsets(payload: FetchOffsetsPayload): Promise<Offset[]>
    resetOffsets(payload: ResetOffsetsPayload): Promise<void>
    setOffsets(payload: SetOffsetsPayload): Promise<void>;
}

export class Kafka {
    constructor(options: ConstructorOptions);
    producer(options?: ProducerOptions): Producer;
    consumer(options?: ConsumerOptions): Consumer;
    admin(options?: AdminOptions): Admin;
    logger(...args: any[]): void;
}

export const AssignerProtocol: {
    MemberAssignment: {
        decode: any;
        encode: any;
    };
    MemberMetadata: {
        decode: any;
        encode: any;
    };
};

export const CompressionTypes: {
    GZIP: number;
    LZ4: number;
    None: number;
    Snappy: number;
};
export const logLevel: {
    DEBUG: number;
    ERROR: number;
    INFO: number;
    NOTHING: number;
    WARN: number;
};
export namespace CompressionCodecs {
}
export namespace Kafka {
    function admin(...args: any[]): void;
    function consumer(...args: any[]): void;
    function logger(...args: any[]): void;
    function producer(...args: any[]): void;
}
export namespace PartitionAssigners {
    function roundRobin({ cluster }: any): any;
}
